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

//! [`MemTable`] for querying `Vec<RecordBatch>` by DataFusion.

use arrow_array::Int32Array;
use datafusion_expr::{BinaryExpr, Operator};
use datafusion_physical_plan::functions::create_physical_expr;
use datafusion_physical_plan::metrics::MetricsSet;
use futures::StreamExt;
use log::debug;
use std::any::Any;
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Debug};
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion_common::{
    not_impl_err, plan_err, Constraints, DFSchema, DataFusionError, SchemaExt,
};
use datafusion_execution::TaskContext;
use parking_lot::Mutex;
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::insert::{DataSink, FileSinkExec};
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::{common, SendableRecordBatchStream};
use datafusion::physical_plan::{repartition::RepartitionExec, Partitioning};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::physical_planner::create_physical_sort_expr;

/// Type alias for partition data
pub type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

type TupletOffset = (i32, i32, i32);

/// In-memory data source for presenting a `Vec<RecordBatch>` as a
/// data source that can be queried by DataFusion. This allows data to
/// be pre-loaded into memory and then repeatedly queried without
/// incurring additional file I/O overhead.
#[derive(Debug)]
pub struct MemTable {
    schema: SchemaRef,
    pub(crate) batches: Vec<PartitionData>,
    constraints: Constraints,
    column_defaults: HashMap<String, Expr>,
    // TODO: Allow primary key to be something other than i32
    primary_key_index: Arc<RwLock<BTreeMap<i32, TupletOffset>>>,
    /// Optional pre-known sort order(s). Must be `SortExpr`s.
    /// inserting data into this table removes the order
    pub sort_order: Arc<Mutex<Vec<Vec<Expr>>>>,
}

impl MemTable {
    /// Create a new in-memory table from the provided schema and record batches
    pub fn try_new(schema: SchemaRef, partitions: Vec<Vec<RecordBatch>>) -> Result<Self> {
        for batches in partitions.iter().flatten() {
            let batches_schema = batches.schema();
            if !schema.contains(&batches_schema) {
                debug!(
                    "mem table schema does not contain batches schema. \
                        Target_schema: {schema:?}. Batches Schema: {batches_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        let mut primary_key_index = BTreeMap::new();

        let primary_key_name = schema
            .metadata()
            .get("primary_key")
            .expect("every table must have a primary key");

        for (partition_idx, batch_idx, batches) in
            partitions
                .iter()
                .enumerate()
                .flat_map(|(partition_idx, partition)| {
                    partition
                        .iter()
                        .enumerate()
                        .map(move |(batch_idx, batches)| (partition_idx, batch_idx, batches))
                })
        {
            let values = batches
                .column_by_name(primary_key_name)
                .expect("table must have primary key column")
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("failed to downcast")
                .values();

            for (value_idx, value) in values.iter().enumerate() {
                if primary_key_index
                    .insert(
                        *value,
                        (partition_idx as i32, batch_idx as i32, value_idx as i32),
                    )
                    .is_some()
                {
                    return plan_err!("Duplicate primary key value.");
                }
            }
        }

        Ok(Self {
            schema,
            batches: partitions
                .into_iter()
                .map(|e| Arc::new(RwLock::new(e)))
                .collect::<Vec<_>>(),
            constraints: Constraints::empty(),
            column_defaults: HashMap::new(),
            primary_key_index: Arc::new(RwLock::new(primary_key_index)),
            sort_order: Arc::new(Mutex::new(vec![])),
        })
    }

    fn supported_filter(&self, expr: &Expr) -> bool {
        if let Expr::BinaryExpr(binary_expr) = expr {
            if let (lhs, Operator::Eq, rhs) =
                (&binary_expr.left, binary_expr.op, &binary_expr.right)
            {
                if let (Expr::Column(c), Expr::Literal(_)) = (lhs.as_ref(), rhs.as_ref()) {
                    &c.name
                        == self
                            .schema()
                            .metadata()
                            .get("primary_key")
                            .expect("primary key is required")
                } else if let (Expr::Literal(_), Expr::Column(c)) = (lhs.as_ref(), rhs.as_ref()) {
                    &c.name
                        == self
                            .schema()
                            .metadata()
                            .get("primary_key")
                            .expect("primary key is required")
                } else {
                    false
                }
            } else {
                false
            }
        } else {
            false
        }
    }

    fn primary_key_filter(&self, expr: &Expr) -> Option<i32> {
        if let Expr::BinaryExpr(binary_expr) = expr {
            if let (lhs, Operator::Eq, rhs) =
                (&binary_expr.left, binary_expr.op, &binary_expr.right)
            {
                if let (Expr::Column(c), Expr::Literal(l)) = (lhs.as_ref(), rhs.as_ref()) {
                    if &c.name
                        == self
                            .schema()
                            .metadata()
                            .get("primary_key")
                            .expect("primary key is required")
                    {
                        match l {
                            datafusion::scalar::ScalarValue::Int32(v) => *v,
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else if let (Expr::Literal(l), Expr::Column(c)) = (lhs.as_ref(), rhs.as_ref()) {
                    if &c.name
                        == self
                            .schema()
                            .metadata()
                            .get("primary_key")
                            .expect("primary key is required")
                    {
                        match l {
                            datafusion::scalar::ScalarValue::Int32(v) => *v,
                            _ => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Assign constraints
    pub fn with_constraints(mut self, constraints: Constraints) -> Self {
        self.constraints = constraints;
        self
    }

    /// Assign column defaults
    pub fn with_column_defaults(mut self, column_defaults: HashMap<String, Expr>) -> Self {
        self.column_defaults = column_defaults;
        self
    }

    /// Specify an optional pre-known sort order(s). Must be `SortExpr`s.
    ///
    /// If the data is not sorted by this order, DataFusion may produce
    /// incorrect results.
    ///
    /// DataFusion may take advantage of this ordering to omit sorts
    /// or use more efficient algorithms.
    ///
    /// Note that multiple sort orders are supported, if some are known to be
    /// equivalent,
    pub fn with_sort_order(self, mut sort_order: Vec<Vec<Expr>>) -> Self {
        std::mem::swap(self.sort_order.lock().as_mut(), &mut sort_order);
        self
    }

    /// Create a mem table by reading from another data source
    pub async fn load(
        t: Arc<dyn TableProvider>,
        output_partitions: Option<usize>,
        state: &SessionState,
    ) -> Result<Self> {
        let schema = t.schema();
        let exec = t.scan(state, None, &[], None).await?;
        let partition_count = exec.output_partitioning().partition_count();

        let mut join_set = JoinSet::new();

        for part_idx in 0..partition_count {
            let task = state.task_ctx();
            let exec = exec.clone();
            join_set.spawn(async move {
                let stream = exec.execute(part_idx, task)?;
                common::collect(stream).await
            });
        }

        let mut data: Vec<Vec<RecordBatch>> =
            Vec::with_capacity(exec.output_partitioning().partition_count());

        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(res) => data.push(res?),
                Err(e) => {
                    if e.is_panic() {
                        std::panic::resume_unwind(e.into_panic());
                    } else {
                        unreachable!();
                    }
                }
            }
        }

        let exec = MemoryExec::try_new(&data, schema.clone(), None)?;

        if let Some(num_partitions) = output_partitions {
            let exec = RepartitionExec::try_new(
                Arc::new(exec),
                Partitioning::RoundRobinBatch(num_partitions),
            )?;

            // execute and collect results
            let mut output_partitions = vec![];
            for i in 0..exec.output_partitioning().partition_count() {
                // execute this *output* partition and collect all batches
                let task_ctx = state.task_ctx();
                let mut stream = exec.execute(i, task_ctx)?;
                let mut batches = vec![];
                while let Some(result) = stream.next().await {
                    batches.push(result?);
                }
                output_partitions.push(batches);
            }

            return MemTable::try_new(schema.clone(), output_partitions);
        }
        MemTable::try_new(schema.clone(), data)
    }
}

#[async_trait]
impl TableProvider for MemTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn constraints(&self) -> Option<&Constraints> {
        Some(&self.constraints)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Fill out the full set of operators we can optimize with our index
        let primary_key_filter = filters
            .iter()
            .find_map(|expr| self.primary_key_filter(expr));
        if let Some(i) = primary_key_filter {
            let primary_key_index = self.primary_key_index.read().await;
            if let Some((partition_idx, batch_idx, value_idx)) = (*primary_key_index).get(&i) {
                let batches = self.batches[*partition_idx as usize].read().await;
                let batch = &batches[*batch_idx as usize];
                let partitions = vec![vec![batch.slice(*value_idx as usize, 1)]];
                let exec = MemoryExec::try_new(&partitions, self.schema(), projection.cloned())?;
                return Ok(Arc::new(exec));
            }
        }
        // TODO: Use tree that supports duplicate keys
        let mut partitions = vec![];
        for arc_inner_vec in self.batches.iter() {
            let inner_vec = arc_inner_vec.read().await;
            partitions.push(inner_vec.clone())
        }
        let mut exec = MemoryExec::try_new(&partitions, self.schema(), projection.cloned())?;

        // add sort information if present
        let sort_order = self.sort_order.lock();
        if !sort_order.is_empty() {
            let df_schema = DFSchema::try_from(self.schema.as_ref().clone())?;

            let file_sort_order = sort_order
                .iter()
                .map(|sort_exprs| {
                    sort_exprs
                        .iter()
                        .map(|expr| {
                            create_physical_sort_expr(expr, &df_schema, state.execution_props())
                        })
                        .collect::<Result<Vec<_>>>()
                })
                .collect::<Result<Vec<_>>>()?;
            exec = exec.with_sort_information(file_sort_order);
        }

        Ok(Arc::new(exec))
    }

    /// Returns an ExecutionPlan that inserts the execution results of a given [`ExecutionPlan`] into this [`MemTable`].
    ///
    /// The [`ExecutionPlan`] must have the same schema as this [`MemTable`].
    ///
    /// # Arguments
    ///
    /// * `state` - The [`SessionState`] containing the context for executing the plan.
    /// * `input` - The [`ExecutionPlan`] to execute and insert.
    ///
    /// # Returns
    ///
    /// * A plan that returns the number of rows written.
    async fn insert_into(
        &self,
        _state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: Update primary key index here
        //
        // If we are inserting into the table, any sort order may be messed up so reset it here
        *self.sort_order.lock() = vec![];

        // Create a physical plan from the logical plan.
        // Check that the schema of the plan matches the schema of this table.
        if !self
            .schema()
            .logically_equivalent_names_and_types(&input.schema())
        {
            return plan_err!("Inserting query must have the same schema with the table.");
        }
        if overwrite {
            return not_impl_err!("Overwrite not implemented for MemoryTable yet");
        }
        let sink = Arc::new(MemSink::new(self.batches.clone()));
        Ok(Arc::new(FileSinkExec::new(
            input,
            sink,
            self.schema.clone(),
            None,
        )))
    }

    fn get_column_default(&self, column: &str) -> Option<&Expr> {
        self.column_defaults.get(column)
    }
}

/// Implements for writing to a [`MemTable`]
struct MemSink {
    /// Target locations for writing data
    batches: Vec<PartitionData>,
}

impl Debug for MemSink {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemSink")
            .field("num_partitions", &self.batches.len())
            .finish()
    }
}

impl DisplayAs for MemSink {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partition_count = self.batches.len();
                write!(f, "MemoryTable (partitions={partition_count})")
            }
        }
    }
}

impl MemSink {
    fn new(batches: Vec<PartitionData>) -> Self {
        Self { batches }
    }
}

#[async_trait]
impl DataSink for MemSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn metrics(&self) -> Option<MetricsSet> {
        None
    }

    async fn write_all(
        &self,
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> Result<u64> {
        let num_partitions = self.batches.len();

        // buffer up the data round robin style into num_partitions

        let mut new_batches = vec![vec![]; num_partitions];
        let mut i = 0;
        let mut row_count = 0;
        while let Some(batch) = data.next().await.transpose()? {
            row_count += batch.num_rows();
            new_batches[i].push(batch);
            i = (i + 1) % num_partitions;
        }

        // write the outputs into the batches
        for (target, mut batches) in self.batches.iter().zip(new_batches.into_iter()) {
            // Append all the new batches in one go to minimize locking overhead
            target.write().await.append(&mut batches);
        }

        Ok(row_count as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema, UInt64Type};
    use arrow::error::ArrowError;
    use datafusion::datasource::provider_as_source;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_common::Column;
    use datafusion_expr::LogicalPlanBuilder;
    use futures::StreamExt;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
                Field::new("d", DataType::Int32, true),
            ],
            schema_metadata,
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;
        println!(
            "primary key index: {:?}",
            provider.primary_key_index.read().await
        );

        // scan with projection
        let exec = provider
            .scan(&session_ctx.state(), Some(&vec![2, 1]), &[], None)
            .await?;

        let mut it = exec.execute(0, task_ctx)?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_with_primary_key_filter() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
                Field::new("d", DataType::Int32, true),
            ],
            schema_metadata,
        ));
        // Create and register the initial table with the provided schema and data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
            ],
        )?;

        let provider = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);
        session_ctx.register_table("t", provider.clone())?;

        // scan with projection
        let column = datafusion_expr::Expr::Column(Column::from_qualified_name("t.a"));
        let literal =
            datafusion_expr::Expr::Literal(datafusion_common::ScalarValue::Int32(Some(1)));
        let filter = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(column),
            op: Operator::Eq,
            right: Box::new(literal),
        });
        let exec = provider
            .scan(&session_ctx.state(), Some(&vec![2, 1]), &[filter], None)
            .await?;

        let mut it = exec.execute(0, task_ctx)?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(1, batch2.num_rows());

        Ok(())
    }
    #[tokio::test]
    async fn test_without_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata,
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let exec = provider.scan(&session_ctx.state(), None, &[], None).await?;
        let mut it = exec.execute(0, task_ctx)?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_invalid_projection() -> Result<()> {
        let session_ctx = SessionContext::new();

        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata,
        ));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let provider = MemTable::try_new(schema, vec![vec![batch]])?;

        let projection: Vec<usize> = vec![0, 4];

        match provider
            .scan(&session_ctx.state(), Some(&projection), &[], None)
            .await
        {
            Err(DataFusionError::ArrowError(ArrowError::SchemaError(e), _)) => {
                assert_eq!(
                    "\"project index 4 out of bounds, max field 3\"",
                    format!("{e:?}")
                )
            }
            res => panic!("Scan should failed on invalid projection, got {res:?}"),
        };

        Ok(())
    }

    #[test]
    fn test_schema_validation_incompatible_column() -> Result<()> {
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema1 = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata.clone(),
        ));

        let schema2 = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Float64, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata,
        ));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let e = MemTable::try_new(schema2, vec![vec![batch]]).unwrap_err();
        assert_eq!(
            "Error during planning: Mismatch between schema and batches",
            e.strip_backtrace()
        );

        Ok(())
    }

    #[test]
    fn test_schema_validation_different_column_count() -> Result<()> {
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema1 = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata.clone(),
        ));

        let schema2 = Arc::new(Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            schema_metadata,
        ));

        let batch = RecordBatch::try_new(
            schema1,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![7, 5, 9])),
            ],
        )?;

        let e = MemTable::try_new(schema2, vec![vec![batch]]).unwrap_err();
        assert_eq!(
            "Error during planning: Mismatch between schema and batches",
            e.strip_backtrace()
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_merged_schema() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let mut metadata = HashMap::new();
        metadata.insert("foo".to_string(), "bar".to_string());
        metadata.insert("primary_key".to_string(), "a".to_string());

        let schema1 = Schema::new_with_metadata(
            vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Int32, false),
                Field::new("c", DataType::Int32, false),
            ],
            // test for comparing metadata
            metadata,
        );

        let schema2 = Schema::new(vec![
            // test for comparing nullability
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, false),
        ]);

        let merged_schema = Schema::try_merge(vec![schema1.clone(), schema2.clone()])?;

        let batch1 = RecordBatch::try_new(
            Arc::new(schema1),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![7, 8, 9])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::new(schema2),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )?;

        let provider = MemTable::try_new(Arc::new(merged_schema), vec![vec![batch1, batch2]])?;

        let exec = provider.scan(&session_ctx.state(), None, &[], None).await?;
        let mut it = exec.execute(0, task_ctx)?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(3, batch1.schema().fields().len());
        assert_eq!(3, batch1.num_columns());

        Ok(())
    }

    async fn experiment(
        schema: SchemaRef,
        initial_data: Vec<Vec<RecordBatch>>,
        inserted_data: Vec<Vec<RecordBatch>>,
    ) -> Result<Vec<Vec<RecordBatch>>> {
        let expected_count: u64 = inserted_data
            .iter()
            .flat_map(|batches| batches.iter().map(|batch| batch.num_rows() as u64))
            .sum();

        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create and register the initial table with the provided schema and data
        let initial_table = Arc::new(MemTable::try_new(schema.clone(), initial_data)?);
        session_ctx.register_table("t", initial_table.clone())?;
        // Create and register the source table with the provided schema and inserted data
        let source_table = Arc::new(MemTable::try_new(schema.clone(), inserted_data)?);
        session_ctx.register_table("source", source_table.clone())?;
        // Convert the source table into a provider so that it can be used in a query
        let source = provider_as_source(source_table);
        // Create a table scan logical plan to read from the source table
        let scan_plan = LogicalPlanBuilder::scan("source", source, None)?.build()?;
        // Create an insert plan to insert the source data into the initial table
        let insert_into_table =
            LogicalPlanBuilder::insert_into(scan_plan, "t", &schema, false)?.build()?;
        // Create a physical plan from the insert plan
        let plan = session_ctx
            .state()
            .create_physical_plan(&insert_into_table)
            .await?;

        // Execute the physical plan and collect the results
        let res = collect(plan, session_ctx.task_ctx()).await?;
        assert_eq!(extract_count(res), expected_count);

        // Read the data from the initial table and store it in a vector of partitions
        let mut partitions = vec![];
        for partition in initial_table.batches.iter() {
            let part = partition.read().await.clone();
            partitions.push(part);
        }
        Ok(partitions)
    }

    /// Returns the value of results. For example, returns 6 given the follwing
    ///
    /// ```text
    /// +-------+,
    /// | count |,
    /// +-------+,
    /// | 6     |,
    /// +-------+,
    /// ```
    fn extract_count(res: Vec<RecordBatch>) -> u64 {
        assert_eq!(res.len(), 1, "expected one batch, got {}", res.len());
        let batch = &res[0];
        assert_eq!(
            batch.num_columns(),
            1,
            "expected 1 column, got {}",
            batch.num_columns()
        );
        let col = batch.column(0).as_primitive::<UInt64Type>();
        assert_eq!(col.len(), 1, "expected 1 row, got {}", col.len());
        let val = col
            .iter()
            .next()
            .expect("had value")
            .expect("expected non null");
        val
    }

    fn build_test_batch(schema: SchemaRef, pk: i32) -> RecordBatch {
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![pk, pk + 1, pk + 2]))],
        )
        .expect("could not create test batch")
    }

    // Test inserting a single batch of data into a single partition
    #[tokio::test]
    async fn test_insert_into_single_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            schema_metadata,
        ));

        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema.clone(),
            vec![vec![build_test_batch(schema.clone(), 1)]],
            vec![vec![build_test_batch(schema.clone(), 2)]],
        )
        .await?;
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 2);
        Ok(())
    }

    // Test inserting multiple batches of data into a single partition
    #[tokio::test]
    async fn test_insert_into_single_partition_with_multi_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            schema_metadata,
        ));

        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema.clone(),
            vec![vec![build_test_batch(schema.clone(), 1)]],
            vec![
                vec![build_test_batch(schema.clone(), 4)],
                vec![build_test_batch(schema.clone(), 7)],
            ],
        )
        .await?;
        // Ensure that the table now contains three batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 3);
        Ok(())
    }

    // Test inserting multiple batches of data into multiple partitions
    #[tokio::test]
    async fn test_insert_into_multi_partition_with_multi_partition() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            schema_metadata,
        ));

        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema.clone(),
            vec![
                vec![build_test_batch(schema.clone(), 1)],
                vec![build_test_batch(schema.clone(), 4)],
            ],
            vec![
                vec![
                    build_test_batch(schema.clone(), 7),
                    build_test_batch(schema.clone(), 10),
                ],
                vec![
                    build_test_batch(schema.clone(), 13),
                    build_test_batch(schema.clone(), 16),
                ],
            ],
        )
        .await?;
        // Ensure that each partition in the table now contains three batches of data
        assert_eq!(resulting_data_in_table[0].len(), 3);
        assert_eq!(resulting_data_in_table[1].len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_from_empty_table() -> Result<()> {
        // Create a new schema with one field called "a" of type Int32
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("primary_key".to_string(), "a".to_string());
        let schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("a", DataType::Int32, false)],
            schema_metadata,
        ));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )?;
        // Run the experiment and obtain the resulting data in the table
        let resulting_data_in_table = experiment(
            schema.clone(),
            vec![vec![
                build_test_batch(schema.clone(), 1),
                build_test_batch(schema.clone(), 4),
            ]],
            vec![vec![]],
        )
        .await?;
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(resulting_data_in_table[0].len(), 2);
        Ok(())
    }
}
