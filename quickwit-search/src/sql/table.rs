use crate::SearchError;
use crate::SearchService;
use arrow::array::BooleanArray;
use arrow::array::PrimitiveArray;
use arrow::array::StringArray;
use arrow::datatypes::Schema as ArrowSchema;
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::arrow::datatypes::DataType;
use datafusion::datasource::datasource::{TableProvider, TableType};
use datafusion::error::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::project_schema;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::RecordBatchStream;
use datafusion::physical_plan::Statistics;
use futures::Stream;
use pin_project::pin_project;
use quickwit_proto::LeafHit;
use quickwit_proto::SearchRequest;
use serde_json::{Result as SerdeResult, Value};
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;
use std::task::Poll;
use tantivy::schema::{FieldType, Schema};
use tokio_stream::StreamMap;

pub struct QuickwitTableProvider {
    schema: Schema,
    search_service: Arc<dyn SearchService>,
    request: SearchRequest,
}

impl QuickwitTableProvider {
    #[allow(dead_code)]
    pub fn new(
        schema: Schema,
        search_service: Arc<dyn SearchService>,
        request: SearchRequest,
    ) -> QuickwitTableProvider {
        QuickwitTableProvider {
            schema,
            search_service,
            request,
        }
    }
}

pub struct QuickwitExecutionPlan {
    schema: Arc<ArrowSchema>,
    recv: std::sync::Mutex<
        Option<
            StreamMap<
                usize,
                std::pin::Pin<
                    Box<
                        dyn Stream<Item = std::result::Result<Vec<LeafHit>, SearchError>>
                            + Sync
                            + Send
                            + 'static,
                    >,
                >,
            >,
        >,
    >,
    projection: Option<Vec<usize>>,
}

#[pin_project]
pub struct QuickwitTableStream {
    schema: Arc<ArrowSchema>,
    #[pin]
    recv: StreamMap<
        usize,
        std::pin::Pin<
            Box<
                dyn Stream<Item = std::result::Result<Vec<LeafHit>, SearchError>>
                    + Sync
                    + Send
                    + 'static,
            >,
        >,
    >,
}

impl Stream for QuickwitTableStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let schema = self.schema.clone();
        let project = self.project();
        let pinned_recv = project.recv;

        let poll_res = pinned_recv.poll_next(cx);
        match poll_res {
            Poll::Pending => Poll::Pending,
            Poll::Ready(hits) => {
                if let Some(hits) = hits {
                    let fields = schema.fields();
                    let mut fields_map: BTreeMap<String, Vec<Box<dyn std::any::Any>>> =
                        BTreeMap::new();

                    for field in fields.into_iter() {
                        fields_map.insert(field.name().clone(), Vec::new());
                    }

                    match hits.1 {
                        Ok(hits) => {
                            hits.into_iter()
                                .map(|hit| {
                                    let res: SerdeResult<Value> =
                                        serde_json::from_str(&hit.leaf_json);
                                    res
                                })
                                .filter(|r| r.is_ok())
                                .map(|r| r.unwrap())
                                .for_each(|row_obj| {
                                    if let Value::Object(mut row_obj) = row_obj {
                                        fields.iter().for_each(|field| {
                                            let data_ary = fields_map.get_mut(field.name());
                                            if let Some(v) = data_ary {
                                                let val = row_obj.remove_entry(field.name());
                                                if let Some((_field_name, val)) = val {
                                                    if !val.is_array() {
                                                        return;
                                                    }
                                                    let ary = val.as_array().unwrap();
                                                    if ary.len() != 1 {
                                                        return;
                                                    }
                                                    if let Value::Array(mut real_val) = val {
                                                        if real_val.len() != 1 {
                                                            return;
                                                        }
                                                        if let Some(real_val) = real_val.pop() {
                                                            v.push(Box::new(real_val));
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                });
                        }
                        Err(e) => {
                            return Poll::Ready(Some(ArrowResult::Err(
                                arrow::error::ArrowError::ComputeError(format!(
                                    "Search quickwit error:{:?}",
                                    e
                                )),
                            )));
                        }
                    }

                    let mut final_column_ary: Vec<Arc<dyn arrow::array::Array + 'static>> =
                        Vec::new();
                    fields_map.into_iter().for_each(|(field_name, v)| {
                        let res = schema.field_with_name(field_name.as_str()).unwrap();
                        match res.data_type() {
                            DataType::Date64 => {
                                let ary = PrimitiveArray::from_iter(
                                    v.into_iter().map(|any| *any.downcast::<i64>().unwrap()),
                                );
                                final_column_ary.push(Arc::new(ary));
                            }
                            DataType::Int64 => {
                                let ary = PrimitiveArray::from_iter(
                                    v.into_iter().map(|any| *any.downcast::<i64>().unwrap()),
                                );
                                final_column_ary.push(Arc::new(ary));
                            }
                            DataType::Float64 => {
                                let ary = PrimitiveArray::from_iter(
                                    v.into_iter().map(|any| *any.downcast::<f64>().unwrap()),
                                );
                                final_column_ary.push(Arc::new(ary));
                            }
                            DataType::Utf8 => {
                                let a = v
                                    .into_iter()
                                    .map(|any| *any.downcast::<String>().unwrap())
                                    .collect::<Vec<String>>();
                                let ary = StringArray::from(a);
                                final_column_ary.push(Arc::new(ary));
                            }
                            DataType::UInt64 => {
                                let ary = PrimitiveArray::from_iter(
                                    v.into_iter().map(|any| *any.downcast::<u64>().unwrap()),
                                );
                                final_column_ary.push(Arc::new(ary));
                            }
                            DataType::Boolean => {
                                let ary = BooleanArray::from(
                                    v.into_iter()
                                        .map(|any| *any.downcast::<bool>().unwrap())
                                        .collect::<Vec<bool>>(),
                                );
                                final_column_ary.push(Arc::new(ary));
                            }
                            _ => {
                                unreachable!()
                            }
                        }
                    });
                    let recordbatch = RecordBatch::try_new(schema, final_column_ary);
                    return Poll::Ready(Some(recordbatch));
                } else {
                    Poll::Ready(None)
                }
            }
        }
    }
}

fn row_to_column_convert() {}

impl RecordBatchStream for QuickwitTableStream {
    fn schema(&self) -> arrow::datatypes::SchemaRef {
        println!(
            "QuickwitTableStream's schema is called:{:?}",
            self.schema.clone()
        );
        self.schema.clone()
    }
}

impl Debug for QuickwitExecutionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuickwitExecutionPlan")
            .field("schema", &self.schema)
            .finish()
    }
}

impl ExecutionPlan for QuickwitExecutionPlan {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        project_schema(&self.schema, self.projection.as_ref()).unwrap()
    }

    // Use datafusion default parallelism
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::RoundRobinBatch(1)
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let mut guard = self.recv.lock().unwrap();
        if guard.is_some() {
            let stream_map = guard.take().unwrap();
            let schema = self.schema.clone();
            let stream = QuickwitTableStream {
                schema,
                recv: stream_map,
            };
            datafusion::error::Result::Ok(Box::pin(stream))
        } else {
            Err(DataFusionError::Plan(
                "Not supported to execute twice for quickwit table".to_string(),
            ))
        }
    }

    fn statistics(&self) -> datafusion::physical_plan::Statistics {
        Statistics::default()
    }
}

pub fn schema_convert(tantivy_schema: &Schema) -> Arc<ArrowSchema> {
    let fields = tantivy_schema.fields();
    let mut df_fields = Vec::new();
    for (_field, entry) in fields {
        match &entry.field_type() {
            FieldType::Bool(_) => {
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::Boolean, true);
                df_fields.push(df_field);
            }
            FieldType::U64(_) => {
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::UInt64, true);
                df_fields.push(df_field);
            }
            FieldType::I64(_) => {
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::Int64, true);
                df_fields.push(df_field);
            }
            FieldType::F64(_) => {
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::Float64, true);
                df_fields.push(df_field);
            }
            FieldType::Date(_) => {
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::Date64, true);
                df_fields.push(df_field);
            }
            // Text data default utf-8
            FieldType::Str(opt) => {
                if !opt.is_stored() {
                    continue;
                }
                let df_field = arrow::datatypes::Field::new(entry.name(), DataType::Utf8, true);
                df_fields.push(df_field);
            }
            _ => {
                continue;
            }
        }
    }
    Arc::new(ArrowSchema::new(df_fields))
}

#[async_trait]
impl TableProvider for QuickwitTableProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        schema_convert(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Temporary
    }

    async fn scan(
        &self,
        _state: &SessionState,
        proj: &Option<Vec<usize>>,
        _filters: &[Expr],
        _: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        println!("Scan is called, projection is proj:{:?}", proj);
        let res = self
            .search_service
            .root_search_sql_stream_leaf_hits(self.request.clone())
            .await;
        match res {
            Ok(s) => Ok(Arc::new(QuickwitExecutionPlan {
                schema: self.schema(),
                recv: Mutex::new(Some(s)),
                projection: proj.clone(),
            })),
            Err(e) => Err(DataFusionError::Internal(format!(
                "Scan quickwit table data failed, search_error:{:?}",
                e
            ))),
        }
    }

    fn supports_filter_pushdown(&self, _: &Expr) -> Result<TableProviderFilterPushDown> {
        Ok(TableProviderFilterPushDown::Unsupported)
    }
}
