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

use super::utils::{make_renamed_schema, rename_expressions};
use super::{DefaultSubstraitConsumer, SubstraitConsumer};
use crate::extensions::Extensions;
use datafusion::common::{not_impl_err, plan_err};
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Aggregate, LogicalPlan, Projection, col};
use std::sync::Arc;
use substrait::proto::{Plan, plan_rel};

/// Convert Substrait Plan to DataFusion LogicalPlan
pub async fn from_substrait_plan(
    state: &SessionState,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    // Register function extension
    let extensions = Extensions::try_from(&plan.extensions)?;
    if !extensions.type_variations.is_empty() {
        return not_impl_err!("Type variation extensions are not supported");
    }

    let consumer = DefaultSubstraitConsumer::with_plan_size(
        &extensions,
        state,
        plan.relations.len(),
    );
    from_substrait_plan_with_consumer(&consumer, plan).await
}

/// Apply root names to a plan, renaming the output schema as specified.
fn apply_root_names(
    plan: LogicalPlan,
    names: &Vec<String>,
) -> datafusion::common::Result<LogicalPlan> {
    if names.is_empty() {
        // Backwards compatibility for plans missing names
        return Ok(plan);
    }
    let renamed_schema = make_renamed_schema(plan.schema(), names)?;
    if renamed_schema
        .has_equivalent_names_and_types(plan.schema())
        .is_ok()
    {
        // Nothing to do if the schema is already equivalent
        return Ok(plan);
    }
    match plan {
        // If the last node of the plan produces expressions, bake the renames into those expressions.
        // This isn't necessary for correctness, but helps with roundtrip tests.
        LogicalPlan::Projection(p) => Ok(LogicalPlan::Projection(Projection::try_new(
            rename_expressions(p.expr, p.input.schema(), renamed_schema.fields())?,
            p.input,
        )?)),
        LogicalPlan::Aggregate(a) => {
            let (group_fields, expr_fields) =
                renamed_schema.fields().split_at(a.group_expr.len());
            let new_group_exprs =
                rename_expressions(a.group_expr, a.input.schema(), group_fields)?;
            let new_aggr_exprs =
                rename_expressions(a.aggr_expr, a.input.schema(), expr_fields)?;
            Ok(LogicalPlan::Aggregate(Aggregate::try_new(
                a.input,
                new_group_exprs,
                new_aggr_exprs,
            )?))
        }
        // There are probably more plans where we could bake things in, can add them later as needed.
        // Otherwise, add a new Project to handle the renaming.
        _ => Ok(LogicalPlan::Projection(Projection::try_new(
            rename_expressions(
                plan.schema().columns().iter().map(|c| col(c.to_owned())),
                plan.schema(),
                renamed_schema.fields(),
            )?,
            Arc::new(plan),
        )?)),
    }
}

/// Convert Substrait Plan to DataFusion LogicalPlan using the given consumer
pub async fn from_substrait_plan_with_consumer(
    consumer: &impl SubstraitConsumer,
    plan: &Plan,
) -> datafusion::common::Result<LogicalPlan> {
    if plan.relations.is_empty() {
        return plan_err!("Substrait plan has no relations");
    }

    let mut last_plan: Option<LogicalPlan> = None;

    for (index, plan_rel) in plan.relations.iter().enumerate() {
        let rel_type = plan_rel.rel_type.as_ref().ok_or_else(|| {
            datafusion::common::DataFusionError::Plan(
                "Cannot parse plan relation: None".to_string(),
            )
        })?;

        let resolved = match rel_type {
            plan_rel::RelType::Rel(rel) => consumer.consume_rel(rel).await?,
            plan_rel::RelType::Root(root) => {
                let Some(root_input) = root.input.as_ref() else {
                    return plan_err!(
                        "Cannot parse plan relation: Root missing input relation"
                    );
                };
                let inner_plan = consumer.consume_rel(root_input).await?;
                apply_root_names(inner_plan, &root.names)?
            }
        };

        consumer.store_resolved_relation(index, &resolved)?;
        last_plan = Some(resolved);
    }

    last_plan.ok_or_else(|| {
        datafusion::common::DataFusionError::Plan(
            "Substrait plan has no relations".to_string(),
        )
    })
}
