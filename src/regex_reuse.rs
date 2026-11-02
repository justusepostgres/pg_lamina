//! Batch-local reuse for repeated StringView values and scalar regex parameters.
//! View identities are meaningful only within one array. No strings or results
//! survive the invocation, and high-cardinality batches use DataFusion directly.
use datafusion::{
    arrow::{
        array::{Array, StringViewArray, UInt32Array, UInt32Builder},
        compute::take,
        datatypes::DataType,
    },
    common::Result,
    functions::regex::regexpreplace::RegexpReplaceFunc,
    logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature},
};
use std::collections::HashMap;

const MAX_UNIQUE: usize = 16384;
const MAX_ROWS: usize = 131072;

#[derive(Debug, PartialEq, Eq, Hash, Default)]
pub struct RepeatedRegexpReplace {
    inner: RegexpReplaceFunc,
}

/// Use at most MAX_UNIQUE hash entries and MAX_ROWS output indices. A null
/// index carries input nullness without evaluating a fabricated empty string.
fn repeated_indices(array: &StringViewArray) -> Option<(UInt32Array, UInt32Array)> {
    if array.len() < 64 || array.len() > MAX_ROWS {
        return None;
    }
    let limit = MAX_UNIQUE.min(array.len() / 2);
    let mut seen = HashMap::with_capacity(limit.min(1024));
    let mut unique = Vec::new();
    let mut useful_repeats = 0;
    let mut indices = UInt32Builder::with_capacity(array.len());
    for (row, view) in array.views().iter().enumerate() {
        // Plain-encoded or constructed strings usually have distinct views.
        // Avoid scanning a large fraction of such a batch just to fall back.
        // Empty strings alone are too cheap to justify this bookkeeping.
        if row == 512 && useful_repeats < 8 {
            return None;
        }
        if array.is_null(row) {
            indices.append_null();
            continue;
        }
        let index = if let Some(index) = seen.get(view) {
            if (*view as u32) != 0 {
                useful_repeats += 1;
            }
            *index
        } else {
            if seen.len() == limit {
                return None;
            }
            let index = seen.len() as u32;
            seen.insert(*view, index);
            unique.push(row as u32);
            index
        };
        indices.append_value(index);
    }
    // Keep the original error/null behavior for all-null inputs.
    if unique.is_empty() {
        return None;
    }
    Some((UInt32Array::from(unique), indices.finish()))
}

impl ScalarUDFImpl for RepeatedRegexpReplace {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn signature(&self) -> &Signature {
        self.inner.signature()
    }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> {
        self.inner.return_type(types)
    }

    fn invoke_with_args(&self, mut args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !args.args[1..]
            .iter()
            .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
        {
            return self.inner.invoke_with_args(args);
        }
        let ColumnarValue::Array(source) = &args.args[0] else {
            return self.inner.invoke_with_args(args);
        };
        let Some(views) = source.as_any().downcast_ref::<StringViewArray>() else {
            return self.inner.invoke_with_args(args);
        };
        let Some((unique, indices)) = repeated_indices(views) else {
            return self.inner.invoke_with_args(args);
        };
        args.args[0] = ColumnarValue::Array(take(source, &unique, None)?);
        args.number_rows = unique.len();
        let result = self
            .inner
            .invoke_with_args(args)?
            .into_array(unique.len())?;
        Ok(ColumnarValue::Array(take(&result, &indices, None)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::{
        arrow::{
            array::{ArrayRef, StringArray},
            datatypes::Field,
        },
        common::{config::ConfigOptions, ScalarValue},
    };
    use std::sync::Arc;

    fn invoke(
        source: ArrayRef,
        pattern: Option<&str>,
        replacement: Option<&str>,
        flags: Option<Option<&str>>,
    ) {
        let mut args = vec![
            ColumnarValue::Array(source.clone()),
            ColumnarValue::Scalar(ScalarValue::Utf8(pattern.map(str::to_owned))),
            ColumnarValue::Scalar(ScalarValue::Utf8(replacement.map(str::to_owned))),
        ];
        if let Some(flags) = flags {
            args.push(ColumnarValue::Scalar(ScalarValue::Utf8(
                flags.map(str::to_owned),
            )));
        }
        compare(args, source.len());
    }

    fn compare(args: Vec<ColumnarValue>, rows: usize) {
        let inner = RegexpReplaceFunc::default();
        let types: Vec<_> = args.iter().map(ColumnarValue::data_type).collect();
        let fields = types
            .iter()
            .map(|t| Arc::new(Field::new("arg", t.clone(), true)))
            .collect();
        let args = ScalarFunctionArgs {
            args,
            arg_fields: fields,
            number_rows: rows,
            return_field: Arc::new(Field::new(
                "result",
                inner.return_type(&types).unwrap(),
                true,
            )),
            config_options: Arc::new(ConfigOptions::default()),
        };
        let expected = inner
            .invoke_with_args(args.clone())
            .and_then(|x| x.into_array(rows));
        let actual = RepeatedRegexpReplace::default()
            .invoke_with_args(args)
            .and_then(|x| x.into_array(rows));
        match (expected, actual) {
            (Ok(a), Ok(b)) => assert_eq!(a.to_data(), b.to_data()),
            (Err(a), Err(b)) => assert_eq!(a.to_string(), b.to_string()),
            (a, b) => panic!("outcomes differ: {a:?} vs {b:?}"),
        }
    }

    fn repeated() -> ArrayRef {
        let dictionary = StringViewArray::from(vec![
            Some("https://www.example.com/a/b"),
            Some("https://other.example/\ntext"),
            Some("αβ—東京/🦀"),
            Some(""),
            None,
            Some("abc abc"),
            Some("a very long nonmatching string"),
            Some("http://www.example.com/a/b"),
        ]);
        let indices = UInt32Array::from((0..4096).map(|i| i % 8).collect::<Vec<_>>());
        take(&dictionary, &indices, None).unwrap()
    }

    #[test]
    fn regex_semantics_and_sliced_nulls_match_upstream() {
        let source = repeated();
        assert!(repeated_indices(source.as_any().downcast_ref().unwrap()).is_some());
        for source in [source.clone(), source.slice(13, 3333)] {
            for (pattern, replacement, flags) in [
                (Some(r"^https?://(?:www\.)?([^/]+)/.*$"), Some(r"\1"), None),
                (
                    Some(r"(?s)^https?://(?:www\.)?([^/]+)/.*$"),
                    Some(r"\1"),
                    None,
                ),
                (Some("a"), Some("long replacement"), Some(Some("g"))),
                (Some("EXAMPLE"), Some("x"), Some(Some("i"))),
                (Some(""), Some("x"), Some(Some("g"))),
                (Some("(α)(β)"), Some(r"\2\1"), None),
                (Some("["), Some("x"), None),
                (None, Some("x"), None),
                (Some("a"), None, None),
                (Some("a"), Some("x"), Some(None)),
            ] {
                invoke(source.clone(), pattern, replacement, flags);
            }
        }
    }

    #[test]
    fn fallback_and_scalar_shapes_match_upstream() {
        for rows in [0, 1, 63, 64, 16384, MAX_ROWS + 1] {
            let strings: Vec<_> = (0..rows).map(|i| format!("distinct string {i}")).collect();
            let source = Arc::new(StringViewArray::from_iter_values(strings.iter()));
            assert!(repeated_indices(&source).is_none());
            invoke(source, Some("string"), Some("replacement"), None);
        }
        invoke(
            Arc::new(StringViewArray::new_null(128)),
            Some("["),
            Some("x"),
            None,
        );
        invoke(
            Arc::new(StringArray::from(vec![Some("abc"); 128])),
            Some("a"),
            Some("x"),
            None,
        );
        compare(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("abc".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into()))),
            ],
            1,
        );
        compare(
            vec![
                ColumnarValue::Array(repeated()),
                ColumnarValue::Array(Arc::new(StringArray::from(vec!["a"; 4096]))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("x".into()))),
            ],
            4096,
        );
    }

    #[test]
    fn distinct_long_views_are_not_conflated() {
        // Same length and 4-byte prefix, different buffers/offsets.
        let dictionary = StringViewArray::from(vec![
            "https://aaaaaaaa.example/path",
            "https://bbbbbbbb.example/path",
        ]);
        let indices = UInt32Array::from((0..512).map(|i| i % 2).collect::<Vec<_>>());
        let source = take(&dictionary, &indices, None).unwrap();
        let (unique, _) = repeated_indices(source.as_any().downcast_ref().unwrap()).unwrap();
        assert_eq!(unique.len(), 2);
        invoke(source, Some("a+"), Some("X"), Some(Some("g")));
    }
}
