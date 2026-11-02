//! Eligibility and reference projection for a future two-pass execution path.
use datafusion::{
    common::tree_node::{TreeNode, TreeNodeRecursion},
    error::Result,
    functions::core::{file_row_index, input_file_name},
    logical_expr::{Expr, FetchType, LogicalPlan, LogicalPlanBuilder, SkipType, Volatility},
};

pub fn references(plan: &LogicalPlan) -> Result<Option<LogicalPlan>> {
    let mut current = plan;
    let mut limit = None;
    while let LogicalPlan::Limit(node) = current {
        if !matches!(node.get_skip_type()?, SkipType::Literal(0)) {
            return Ok(None);
        }
        match node.get_fetch_type()? {
            FetchType::Literal(Some(n)) => limit = Some(limit.map_or(n, |v: usize| v.min(n))),
            FetchType::Literal(None) => {}
            _ => return Ok(None),
        }
        current = &node.input;
    }
    let LogicalPlan::Sort(sort) = current else {
        return Ok(None);
    };
    if let Some(n) = sort.fetch {
        limit = Some(limit.map_or(n, |v| v.min(n)));
    }
    if !limit.is_some_and(|n| (1..=10).contains(&n)) {
        return Ok(None);
    }
    current = &sort.input;
    while let LogicalPlan::Filter(node) = current {
        current = &node.input;
    }
    let LogicalPlan::TableScan(scan) = current else {
        return Ok(None);
    };
    // The selective fetch initially returns complete stored rows only.
    let schema = scan.source.schema();
    if scan.projected_schema.fields() != schema.fields()
        || plan.schema().fields() != schema.fields()
    {
        return Ok(None);
    }
    let mut safe = true;
    plan.apply(|node| {
        for expr in node.expressions() {
            expr.apply(|expr| {
                match expr {
                    Expr::ScalarFunction(f)
                        if f.func.signature().volatility != Volatility::Immutable =>
                    {
                        safe = false
                    }
                    Expr::Exists(_)
                    | Expr::InSubquery(_)
                    | Expr::ScalarSubquery(_)
                    | Expr::OuterReferenceColumn(_, _)
                    | Expr::Placeholder(_) => safe = false,
                    _ => {}
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if !safe {
        return Ok(None);
    }
    // Source-dependent functions are moved into the scan by DataFusion's
    // projection rules; the original filters, ordering and limit remain intact.
    Ok(Some(
        LogicalPlanBuilder::from(plan.clone())
            .project([
                input_file_name().call(vec![]).alias("__late_file"),
                file_row_index().call(vec![]).alias("__late_row"),
            ])?
            .build()?,
    ))
}
