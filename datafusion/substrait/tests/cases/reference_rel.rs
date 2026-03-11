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

//! Tests for ReferenceRel support in multi-relation Substrait plans

#[cfg(test)]
mod tests {
    use crate::utils::test::add_plan_schemas_to_ctx;
    use datafusion::common::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use insta::assert_snapshot;
    use substrait::proto::r#type::Nullability;
    use substrait::proto::read_rel::{NamedTable, ReadType};
    use substrait::proto::rel::RelType;
    use substrait::proto::{
        CrossRel, NamedStruct, Plan, PlanRel, ReadRel, ReferenceRel, Rel, RelRoot,
        r#type,
    };

    fn make_read_rel(table_name: &str, col_name: &str) -> Rel {
        Rel {
            rel_type: Some(RelType::Read(Box::new(ReadRel {
                common: None,
                base_schema: Some(NamedStruct {
                    names: vec![col_name.to_string()],
                    r#struct: Some(r#type::Struct {
                        types: vec![substrait::proto::Type {
                            kind: Some(r#type::Kind::I64(r#type::I64 {
                                type_variation_reference: 0,
                                nullability: Nullability::Required as i32,
                            })),
                        }],
                        type_variation_reference: 0,
                        nullability: Nullability::Required as i32,
                    }),
                }),
                filter: None,
                best_effort_filter: None,
                projection: None,
                advanced_extension: None,
                read_type: Some(ReadType::NamedTable(NamedTable {
                    names: vec![table_name.to_string()],
                    advanced_extension: None,
                })),
            }))),
        }
    }

    fn make_reference_rel(subtree_ordinal: i32) -> Rel {
        Rel {
            rel_type: Some(RelType::Reference(ReferenceRel { subtree_ordinal })),
        }
    }

    fn plan_rel(rel: Rel) -> PlanRel {
        PlanRel {
            rel_type: Some(substrait::proto::plan_rel::RelType::Rel(rel)),
        }
    }

    fn plan_root(rel: Rel, names: Vec<String>) -> PlanRel {
        PlanRel {
            rel_type: Some(substrait::proto::plan_rel::RelType::Root(RelRoot {
                input: Some(rel),
                names,
            })),
        }
    }

    #[expect(deprecated)]
    fn make_plan(relations: Vec<PlanRel>) -> Plan {
        Plan {
            version: None,
            extension_uris: vec![],
            extension_urns: vec![],
            extensions: vec![],
            relations,
            advanced_extensions: None,
            expected_type_urls: vec![],
            parameter_bindings: vec![],
            type_aliases: vec![],
        }
    }

    #[tokio::test]
    async fn test_multi_relation_with_reference_rel_self_cross_join() -> Result<()> {
        let read = make_read_rel("t1", "id");
        let cross = Rel {
            rel_type: Some(RelType::Cross(Box::new(CrossRel {
                common: None,
                left: Some(Box::new(make_reference_rel(0))),
                right: Some(Box::new(make_reference_rel(0))),
                advanced_extension: None,
            }))),
        };
        let plan = make_plan(vec![
            plan_rel(read),
            plan_root(cross, vec!["id".to_string(), "id2".to_string()]),
        ]);

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &plan)?;
        let result = from_substrait_plan(&ctx.state(), &plan).await?;

        assert_snapshot!(
            result,
            @r"
            Projection: left.id, right.id AS id2
              Cross Join:
                SubqueryAlias: left
                  TableScan: t1
                SubqueryAlias: right
                  TableScan: t1
            "
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reference_rel_out_of_bounds() -> Result<()> {
        let plan = make_plan(vec![plan_root(
            make_reference_rel(5),
            vec!["id".to_string()],
        )]);

        let ctx = SessionContext::new();
        let err = from_substrait_plan(&ctx.state(), &plan)
            .await
            .expect_err("plan with out-of-bounds ReferenceRel must fail");

        assert!(
            err.to_string().contains("out of bounds"),
            "Expected out of bounds error, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reference_rel_forward_reference() -> Result<()> {
        let read = make_read_rel("t1", "id");
        let plan = make_plan(vec![
            plan_root(make_reference_rel(1), vec!["id".to_string()]),
            plan_rel(read),
        ]);

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &plan)?;
        let err = from_substrait_plan(&ctx.state(), &plan)
            .await
            .expect_err("plan with forward ReferenceRel must fail");

        assert!(
            err.to_string().contains("not yet resolved")
                || err.to_string().contains("forward"),
            "Expected forward reference error, got: {err}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_single_relation_backward_compat() -> Result<()> {
        let read = make_read_rel("t1", "id");
        let plan = make_plan(vec![plan_root(read, vec!["id".to_string()])]);

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &plan)?;
        let result = from_substrait_plan(&ctx.state(), &plan).await?;

        assert_snapshot!(
            result,
            @"TableScan: t1"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_chained_references() -> Result<()> {
        let read = make_read_rel("t1", "id");
        let cross = Rel {
            rel_type: Some(RelType::Cross(Box::new(CrossRel {
                common: None,
                left: Some(Box::new(make_reference_rel(0))),
                right: Some(Box::new(make_reference_rel(0))),
                advanced_extension: None,
            }))),
        };
        let cross2 = Rel {
            rel_type: Some(RelType::Cross(Box::new(CrossRel {
                common: None,
                left: Some(Box::new(make_reference_rel(1))),
                right: Some(Box::new(make_reference_rel(0))),
                advanced_extension: None,
            }))),
        };
        let plan = make_plan(vec![
            plan_rel(read),
            plan_rel(cross),
            plan_root(
                cross2,
                vec!["id1".to_string(), "id2".to_string(), "id3".to_string()],
            ),
        ]);

        let ctx = add_plan_schemas_to_ctx(SessionContext::new(), &plan)?;
        let result = from_substrait_plan(&ctx.state(), &plan).await?;

        assert_snapshot!(
            result,
            @r"
            Projection: left.id AS id1, right.id AS id2, t1.id AS id3
              Cross Join:
                Cross Join:
                  SubqueryAlias: left
                    TableScan: t1
                  SubqueryAlias: right
                    TableScan: t1
                TableScan: t1
            "
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_reference_rel_negative_ordinal() -> Result<()> {
        let plan = make_plan(vec![plan_root(
            make_reference_rel(-1),
            vec!["id".to_string()],
        )]);

        let ctx = SessionContext::new();
        let err = from_substrait_plan(&ctx.state(), &plan)
            .await
            .expect_err("plan with negative ReferenceRel ordinal must fail");

        assert!(
            err.to_string().contains("negative")
                || err.to_string().contains("Invalid"),
            "Expected negative ordinal error, got: {err}"
        );
        Ok(())
    }
}
