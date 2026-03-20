// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Rewrites `UNION DISTINCT` branches that differ only by filter predicates
//! into a single filtered branch plus `DISTINCT`.

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr_rewriter::coerce_plan_expr_for_schema;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::utils::disjunction;
use datafusion_expr::{
    Distinct, Expr, Filter, LogicalPlan, Projection, SubqueryAlias, Union,
};
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct UnionsToFilter;

impl UnionsToFilter {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for UnionsToFilter {
    fn name(&self) -> &str {
        "unions_to_filter"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_unions_to_filter {
            return Ok(Transformed::no(plan));
        }

        match plan {
            LogicalPlan::Distinct(Distinct::All(input)) => {
                let inner = Arc::unwrap_or_clone(input);
                match try_rewrite_distinct_union(inner.clone())? {
                    Some(rewritten) => Ok(Transformed::yes(rewritten)),
                    None => Ok(Transformed::no(LogicalPlan::Distinct(Distinct::All(
                        Arc::new(inner),
                    )))),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn try_rewrite_distinct_union(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Union(Union { inputs, schema }) = plan else {
        return Ok(None);
    };

    if inputs.len() < 2 {
        return Ok(None);
    }

    let mut grouped: HashMap<GroupKey, Vec<Expr>> = HashMap::new();
    let mut input_order: Vec<GroupKey> = Vec::new();
    let mut transformed = false;

    for input in inputs {
        let Some(branch) = extract_branch(Arc::unwrap_or_clone(input))? else {
            return Ok(None);
        };

        let key = GroupKey {
            source: branch.source,
            wrappers: branch.wrappers,
        };
        if let Some(conds) = grouped.get_mut(&key) {
            conds.push(branch.predicate);
            transformed = true;
        } else {
            input_order.push(key.clone());
            grouped.insert(key, vec![branch.predicate]);
        }
    }

    if !transformed {
        return Ok(None);
    }

    let mut builder: Option<LogicalPlanBuilder> = None;
    for key in input_order {
        let predicates = grouped
            .remove(&key)
            .expect("grouped predicates should exist for every source");
        let combined =
            disjunction(predicates).expect("union branches always provide predicates");
        let branch = LogicalPlanBuilder::from(key.source)
            .filter(combined)?
            .build()?;
        let branch = wrap_branch(branch, &key.wrappers)?;
        let branch = coerce_plan_expr_for_schema(branch, &schema)?;
        let branch = align_plan_to_schema(branch, Arc::clone(&schema))?;
        builder = Some(match builder {
            None => LogicalPlanBuilder::from(branch),
            Some(builder) => builder.union(branch)?,
        });
    }

    let union = builder
        .expect("at least one branch after rewrite")
        .build()?;
    Ok(Some(LogicalPlan::Distinct(Distinct::All(Arc::new(union)))))
}

struct Branch {
    source: LogicalPlan,
    predicate: Expr,
    wrappers: Vec<Wrapper>,
}

fn extract_branch(plan: LogicalPlan) -> Result<Option<Branch>> {
    let (wrappers, plan) = peel_wrappers(plan);
    match plan {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                return Ok(None);
            }
            Ok(Some(Branch {
                source: strip_passthrough_nodes(Arc::unwrap_or_clone(input)),
                predicate,
                wrappers,
            }))
        }
        other => Ok(Some(Branch {
            source: strip_passthrough_nodes(other.clone()),
            predicate: Expr::Literal(
                datafusion_common::ScalarValue::Boolean(Some(true)),
                None,
            ),
            wrappers,
        })),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupKey {
    source: LogicalPlan,
    wrappers: Vec<Wrapper>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Wrapper {
    Projection {
        expr: Vec<Expr>,
        schema: datafusion_common::DFSchemaRef,
    },
    SubqueryAlias {
        alias: datafusion_common::TableReference,
        schema: datafusion_common::DFSchemaRef,
    },
}

fn peel_wrappers(mut plan: LogicalPlan) -> (Vec<Wrapper>, LogicalPlan) {
    let mut wrappers = vec![];
    loop {
        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::Projection { expr, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::SubqueryAlias { alias, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            other => return (wrappers, other),
        }
    }
}

fn wrap_branch(mut plan: LogicalPlan, wrappers: &[Wrapper]) -> Result<LogicalPlan> {
    for wrapper in wrappers.iter().rev() {
        plan = match wrapper {
            Wrapper::Projection { expr, schema } => {
                LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr.clone(),
                    Arc::new(plan),
                    Arc::clone(schema),
                )?)
            }
            Wrapper::SubqueryAlias { alias, .. } => LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), alias.clone())?,
            ),
        };
    }
    Ok(plan)
}

fn strip_passthrough_nodes(plan: LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Projection(Projection { input, .. }) => {
            strip_passthrough_nodes(Arc::unwrap_or_clone(input))
        }
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
            strip_passthrough_nodes(Arc::unwrap_or_clone(input))
        }
        other => other,
    }
}

fn align_plan_to_schema(
    plan: LogicalPlan,
    schema: datafusion_common::DFSchemaRef,
) -> Result<LogicalPlan> {
    if plan.schema() == &schema {
        return Ok(plan);
    }

    let expr = plan
        .schema()
        .iter()
        .enumerate()
        .map(|(i, _)| {
            Expr::Column(datafusion_common::Column::from(
                plan.schema().qualified_field(i),
            ))
        })
        .collect::<Vec<_>>();

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expr,
        Arc::new(plan),
        schema,
    )?))
}

fn is_mergeable_predicate(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_subquery(expr)
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => Ok(true),
        _ => Ok(false),
    })
    .expect("boolean expression walk is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType;
    use datafusion_common::Result;
    use datafusion_expr::{
        ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility, col, lit,
    };
    use std::any::Any;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut options = datafusion_common::config::ConfigOptions::default();
            options.optimizer.enable_unions_to_filter = true;
            let optimizer_ctx = OptimizerContext::new_with_config_options(Arc::new(options))
                .with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(UnionsToFilter::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileTestUdf;

    impl ScalarUDFImpl for VolatileTestUdf {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn name(&self) -> &str {
            "volatile_test"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
            &SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("VolatileTestUdf is not intended for execution")
        }
    }

    fn volatile_expr() -> Expr {
        ScalarUDF::new_from_impl(VolatileTestUdf).call(vec![])
    }

    #[test]
    fn rewrite_union_distinct_same_source_filters() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Projection: t.a, t.b, t.c
            Filter: t.a = Int32(1) OR t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_distinct_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Filter: t1.a = Int32(1)
              TableScan: t1
            Filter: t2.a = Int32(2)
              TableScan: t2
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_distinct_with_volatile_predicate() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(volatile_expr().gt(lit(0.5_f64)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Union
            Filter: volatile_test() > Float64(0.5)
              TableScan: t
            Filter: t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_distinct_with_matching_projection_prefix() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("b").eq(lit(5)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .union_distinct(right)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Distinct:
          Projection: emp.a AS mgr, emp.b AS comm
            Filter: Boolean(true) OR emp.b = Int32(5)
              TableScan: emp
        ")?;
        Ok(())
    }
}
