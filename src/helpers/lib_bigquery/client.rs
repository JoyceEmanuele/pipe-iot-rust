use crate::GlobalVars;
use gcp_bigquery_client::error::{BQError, NestedResponseError};
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::append_rows_response;
use gcp_bigquery_client::model::clustering::Clustering;
use gcp_bigquery_client::model::query_request::QueryRequest;
use gcp_bigquery_client::model::query_response::ResultSet;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::model::table_data_insert_all_request::TableDataInsertAllRequest;
use gcp_bigquery_client::model::table_data_insert_all_response::TableDataInsertAllResponse;
use gcp_bigquery_client::model::table_field_schema::TableFieldSchema;
use gcp_bigquery_client::model::table_schema::TableSchema;
use gcp_bigquery_client::model::time_partitioning::TimePartitioning;
use gcp_bigquery_client::storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor};
use prost_derive::Message;
use serde::Serialize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio_stream::StreamExt;

// type OnInserted = dyn Fn(&Arc<GlobalVars>) + Send + Sync;
type OnTableNotFound = dyn Fn(&Arc<GlobalVars>, &str) + Send + Sync;

pub struct GCPConfig {
    pub credentials_file: String,
    pub project_id: String,
    pub dataset_id: String,
}

pub struct BigQueryClient {
    pub client: gcp_bigquery_client::Client,
    pub project_id: String,
    pub dataset_id: String,
    pub table_descriptor: TableDescriptor,
    // pub on_inserted: Option<&'static OnInserted>,
    pub on_table_not_found: Option<&'static OnTableNotFound>,
}

impl BigQueryClient {
    pub async fn new(
        config: &GCPConfig,
        on_table_not_found: Option<&'static OnTableNotFound>,
    ) -> Result<BigQueryClient, String> {
        // Init BigQuery client
        let client =
            gcp_bigquery_client::Client::from_service_account_key_file(&config.credentials_file)
                .await
                .map_err(|err| format!("[54] {err}"))?;

        let field_descriptors = vec![
            FieldDescriptor {
                name: "timestamp".to_string(),
                number: 1,
                typ: ColumnType::Timestamp,
            },
            FieldDescriptor {
                name: "dev_id".to_string(),
                number: 2,
                typ: ColumnType::String,
            },
            FieldDescriptor {
                name: "payload".to_string(),
                number: 3,
                typ: ColumnType::Json,
            },
        ];

        let table_descriptor = TableDescriptor { field_descriptors };

        Ok(BigQueryClient {
            client,
            project_id: config.project_id.to_owned(),
            dataset_id: config.dataset_id.to_owned(),
            table_descriptor,
            // on_inserted: None,
            on_table_not_found,
        })
    }

    pub fn clone(&self) -> BigQueryClient {
        let field_descriptors = self
            .table_descriptor
            .field_descriptors
            .iter()
            .map(|x| FieldDescriptor {
                name: x.name.to_owned(),
                number: x.number,
                typ: x.typ,
            })
            .collect();
        BigQueryClient {
            client: self.client.clone(),
            project_id: self.project_id.clone(),
            dataset_id: self.dataset_id.clone(),
            table_descriptor: TableDescriptor { field_descriptors },
            // on_inserted: self.on_inserted.clone(),
            on_table_not_found: self.on_table_not_found.clone(),
        }
    }

    pub fn get_project_id<'a>(&'a self) -> &'a str {
        &self.project_id
    }

    pub fn get_dataset_id<'a>(&'a self) -> &'a str {
        &self.dataset_id
    }

    pub async fn create_new_table(&self, table_name: &str) -> Result<Table, String> {
        // Get dataset
        let dataset = self
            .client
            .dataset()
            .get(&self.project_id, &self.dataset_id)
            .await
            .map_err(|err| format!("[122] {err}"))?;

        let mut field_timestamp = TableFieldSchema::timestamp("timestamp");
        let mut field_dev_id = TableFieldSchema::string("dev_id");
        let mut field_payload = TableFieldSchema::json("payload");
        field_timestamp.mode = Some("REQUIRED".to_owned());
        field_dev_id.mode = Some("REQUIRED".to_owned());
        field_payload.mode = Some("REQUIRED".to_owned());
        dataset
            .create_table(
                &self.client,
                Table::from_dataset(
                    &dataset,
                    table_name,
                    TableSchema::new(vec![field_timestamp, field_dev_id, field_payload]),
                )
                .time_partitioning(TimePartitioning::per_day().field("timestamp"))
                .clustering(Clustering {
                    fields: Some(vec!["dev_id".to_owned()]),
                }),
            )
            .await
            .map_err(|err| format!("[144] {err}"))

        // println!("BigQuery table created -> {:?}", resp);
    }

    pub async fn insert_telemetry_by_stream(
        &self,
        row: RowBQ,
        table_name: &str,
        globs: &Arc<GlobalVars>,
    ) -> Result<TableDataInsertAllResponse, String> {
        // Insert data via BigQuery Streaming API
        let mut insert_request = TableDataInsertAllRequest::new();

        insert_request
            .add_row(None, row)
            .map_err(|err| format!("[160] {err}"))?;

        let resp = self
            .client
            .tabledata()
            .insert_all(
                &self.project_id,
                &self.dataset_id,
                table_name,
                insert_request,
            )
            .await;

        if let Err(BQError::ResponseError { error }) = &resp {
            if error.error.message.starts_with("Not found: Table ") {
                if let Some(on_table_not_found) = &self.on_table_not_found {
                    on_table_not_found(globs, table_name);
                }
            }
            // Response error (
            // 	error: ResponseError {
            // 		error: NestedResponseError {
            // 			code: 404,
            // 			errors: [
            // 				{"message": "Not found: Table projectid:datasetname.tablename", "domain": "global", "reason": "notFound"}
            // 			],
            // 			message: "Not found: Table projectid:datasetname.tablename",
            // 			status: "NOT_FOUND"
            // 		}
            // 	}
            // )
        }

        let resp = resp.map_err(|err| format!("[192] {err}"))?;

        if resp.insert_errors.is_some() {
            return Err(format!("{:?}", resp));
        }

        globs
            .stats
            .bigquery_insertions
            .fetch_add(1, Ordering::Relaxed);

        Ok(resp)
    }

    pub async fn insert_telemetry_list_by_storage(
        &mut self,
        rows: &[RowBQ],
        table_name: &str,
        globs: &Arc<GlobalVars>,
    ) -> Result<(), String> {
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            self.dataset_id.clone(),
            table_name.to_owned(),
        );
        let trace_id = "D".to_string();

        let result = self
            .client
            .storage_mut()
            .append_rows(&stream_name, &self.table_descriptor, rows, trace_id)
            .await
            .map_err(|err| format!("[242] {table_name} {err}"));
        let mut streaming = match result {
            Ok(x) => x,
            Err(err) => {
                if err.contains("Permission 'TABLES_UPDATE_DATA' denied")
                    && err.contains("it may not exist")
                {
                    if let Some(on_table_not_found) = &self.on_table_not_found {
                        on_table_not_found(globs, table_name);
                    }
                }
                return Err(err);
            }
        };

        while let Some(resp) = streaming.next().await {
            let resp = match resp.map_err(|err| format!("[256] {table_name} {err}")) {
                Ok(x) => x,
                Err(err) => {
                    if err.contains("Permission 'TABLES_UPDATE_DATA' denied")
                        && err.contains("it may not exist")
                    {
                        if let Some(on_table_not_found) = &self.on_table_not_found {
                            on_table_not_found(globs, table_name);
                        }
                    }
                    return Err(err);
                }
            };
            for item in resp.row_errors {
                crate::LOG.append_log_tag_msg("DBG-ERR", &format!("[281] {}", item.message));
            }
            if let Some(resp) = &resp.response {
                if let append_rows_response::Response::Error(err) = resp {
                    if err
                        .message
                        .contains("Permission 'TABLES_UPDATE_DATA' denied")
                        && err.message.contains("it may not exist")
                    {
                        if let Some(on_table_not_found) = &self.on_table_not_found {
                            on_table_not_found(globs, table_name);
                        }
                    }
                    return Err(format!("[264] {}", err.message));
                }
            }
            // println!("[237]: response {resp:#?}");
        }

        Ok(())
    }

    pub async fn insert_telemetry_json(
        &self,
        row: serde_json::Value,
        table_name: &str,
        globs: &Arc<GlobalVars>,
    ) -> Result<TableDataInsertAllResponse, String> {
        // Insert data via BigQuery Streaming API
        let mut insert_request = TableDataInsertAllRequest::new();

        insert_request
            .add_row(None, row)
            .map_err(|err| format!("[455] {err}"))?;

        let resp = self
            .client
            .tabledata()
            .insert_all(
                &self.project_id,
                &self.dataset_id,
                table_name,
                insert_request,
            )
            .await;

        if let Err(BQError::ResponseError { error }) = &resp {
            if error.error.message.starts_with("Not found: Table ") {
                if let Some(on_table_not_found) = &self.on_table_not_found {
                    on_table_not_found(globs, table_name);
                }
            }
        }

        let resp = resp.map_err(|err| format!("[476] {err}"))?;

        if resp.insert_errors.is_some() {
            return Err(format!("{:?}", resp));
        }

        globs
            .stats
            .bigquery_insertions
            .fetch_add(1, Ordering::Relaxed);

        Ok(resp)
    }

    pub async fn insert_telemetry_list_by_stream(
        &self,
        rows: Vec<RowBQ>,
        table_name: &str,
        globs: &Arc<GlobalVars>,
    ) -> Result<TableDataInsertAllResponse, String> {
        // Insert data via BigQuery Streaming API
        let mut insert_request = TableDataInsertAllRequest::new();

        for row in rows {
            insert_request
                .add_row(None, row)
                .map_err(|err| format!("[505] {err}"))?;
        }

        let resp = self
            .client
            .tabledata()
            .insert_all(
                &self.project_id,
                &self.dataset_id,
                table_name,
                insert_request,
            )
            .await;

        if let Err(BQError::ResponseError { error }) = &resp {
            if error.error.message.starts_with("Not found: Table ") {
                if let Some(on_table_not_found) = &self.on_table_not_found {
                    on_table_not_found(globs, table_name);
                }
            }
        }

        let resp = resp.map_err(|err| format!("[527] {err}"))?;

        if resp.insert_errors.is_some() {
            return Err(format!("{:?}", resp));
        }

        globs
            .stats
            .bigquery_insertions
            .fetch_add(1, Ordering::Relaxed);

        Ok(resp)
    }

    pub async fn example_query(&self, table_name: &str) -> Result<(), String> {
        let query_request = QueryRequest::new(format!(
            "SELECT COUNT(*) AS c FROM `{}.{}.{}`",
            self.project_id, self.dataset_id, table_name
        ));

        // Query
        let query_response = self
            .client
            .job()
            .query(&self.project_id, query_request)
            .await
            .map_err(|err| format!("[556] {err}"))?;
        let mut rs = ResultSet::new_from_query_response(query_response);

        while rs.next_row() {
            println!(
                "Number of rows inserted: {}",
                rs.get_i64_by_name("c")
                    .map_err(|err| format!("[562] {err}"))?
                    .unwrap()
            );
        }

        Ok(())
    }
}

#[derive(Serialize, Message)]
pub struct RowBQ {
    #[prost(string, tag = "1")]
    pub timestamp: String,

    #[prost(string, tag = "2")]
    pub dev_id: String,

    #[prost(string, tag = "3")]
    pub payload: String, // serde_json::Value
}
