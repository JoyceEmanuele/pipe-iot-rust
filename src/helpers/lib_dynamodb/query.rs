use super::client::{AWSConfig, DynamoDBClientDiel};
use rusoto_core::RusotoError;
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, QueryError, QueryInput};
use serde_dynamo::from_items;
use std::collections::HashMap;

pub struct QuerierDevIdTimestamp {
    client: DynamoDbClient,
    table_name: String,

    key_var_name: String, // dev_id
    part_key: String,

    order_var_name: String,
    // ts_ini: String, // page_ts_ini
    // ts_end: String,
    current_query: Option<CurrentQuery>,
}

pub struct CurrentQuery {
    pub query_input: QueryInput,
    pub is_next_page: bool,
    pub acabou: bool,
}

impl QuerierDevIdTimestamp {
    pub fn new_diel_dev(table_name: String, dev_id: String, config: &AWSConfig) -> Self {
        Self {
            client: DynamoDBClientDiel::create_client(config),
            table_name,
            key_var_name: "dev_id".to_owned(),
            order_var_name: "timestamp".to_owned(),
            part_key: dev_id,
            // page_ts_ini: ts_ini,
            // ts_end,
            current_query: None,
        }
    }

    pub fn new_custom(
        table_name: String,
        key_var_name: String,
        order_var_name: String,
        part_key: String,
        config: &AWSConfig,
    ) -> Self {
        Self {
            client: DynamoDBClientDiel::create_client(config),
            table_name,
            key_var_name,
            order_var_name,
            part_key,
            current_query: None,
        }
    }

    async fn fetch_page(
        query_input: QueryInput,
        is_next_page: bool,
        client: &DynamoDbClient,
    ) -> Result<rusoto_dynamodb::QueryOutput, String> {
        let mut retries = 0;
        loop {
            match client.query(query_input.clone()).await {
                Ok(result_page) => {
                    if result_page.items.is_none() {
                        return Err("Query returned no items".to_owned());
                    }
                    return Ok(result_page);
                }
                Err(err) => {
                    match &err {
                        RusotoError::Service(QueryError::ProvisionedThroughputExceeded(
                            err_msg,
                        )) => {
                            if (retries < 2) && (is_next_page) {
                                retries += 1;
                                eprintln!("{}", err_msg);
                                tokio::time::sleep(std::time::Duration::from_millis(2600)).await;
                                continue;
                            } else {
                                return Err(format!("ProvisionedThroughputExceeded: {}", &err_msg));
                            }
                        }
                        RusotoError::Service(QueryError::ResourceNotFound(err_msg)) => {
                            // Table not found
                            return Err(format!("ResourceNotFound: {}", &err_msg));
                        }
                        _ => return Err(err.to_string()),
                    };
                }
            };
        }
    }

    fn create_query_input(
        table_name: &str,
        key_var_name: &str,
        part_key: &str,
        order_var_name: &str,
        page_ts_ini: &str,
        ts_end: &str,
    ) -> QueryInput {
        // order_var_name = timestamp
        // key_var_name = dev_id
        // part_key = dev_id, self.serial
        crate::LOG.append_log_tag_msg(
            "INFO",
            &format!("dynamoQuery: {} {} {}", &table_name, &page_ts_ini, &ts_end),
        );
        return QueryInput {
            table_name: table_name.to_owned(),
            consistent_read: Some(false),
            projection_expression: None, // Some(String::from("#ts,L1,#State,#Mode"))
            key_condition_expression: Some(format!(
                "{key} = :{key} and #ts between :ts_begin and :ts_end",
                key = key_var_name
            )),
            // key_condition_expression: Some(format!("{key} = :{key} and begins_with(#ts, :day)", key = key_var_name)),
            expression_attribute_names: {
                let mut map = HashMap::new();
                map.insert("#ts".to_owned(), order_var_name.to_owned());
                Some(map)
            },
            expression_attribute_values: {
                let mut map = HashMap::new();
                map.insert(
                    format!(":{}", key_var_name),
                    AttributeValue {
                        s: Some(part_key.to_owned()),
                        ..AttributeValue::default()
                    },
                );
                map.insert(
                    ":ts_begin".to_owned(),
                    AttributeValue {
                        s: Some(page_ts_ini.to_owned()),
                        ..AttributeValue::default()
                    },
                );
                map.insert(
                    ":ts_end".to_owned(),
                    AttributeValue {
                        s: Some(ts_end.to_owned()),
                        ..AttributeValue::default()
                    },
                );
                // map.insert(":day".to_owned(),            AttributeValue { s: Some(self.day.to_string()), ..AttributeValue::default() });
                // map.insert(":ts_begin".to_owned(),     AttributeValue { s: Some(self.ts_begin.to_string()), ..AttributeValue::default() });
                // map.insert(":ts_end".to_owned(),       AttributeValue { s: Some(self.ts_end.to_string()), ..AttributeValue::default() });
                Some(map)
            },
            ..QueryInput::default()
        };
    }

    pub async fn run<'a, T, F>(
        &self,
        ts_ini: &str,
        ts_end: &str,
        proc_items: &mut F,
    ) -> Result<(), String>
    where
        T: serde::Deserialize<'a>, // serde_json::Value
        F: FnMut(Vec<T>) -> Result<(), String>,
        F: Send,
    {
        // T = serde_json::Value
        // let create_query = {
        //   let _table_name = self.table_name.to_owned();
        //   let _dev_id = self.dev_id.to_owned();
        //   let _ts_end = self.ts_end.to_owned();
        //   move |page_ts_ini_2: &str| {
        //     let ini = page_ts_ini_2.to_string();
        //     Self::create_query_input(&_table_name, &_dev_id, &ini, &_ts_end)
        //   }
        // };
        let mut query_input = Self::create_query_input(
            &self.table_name,
            &self.key_var_name,
            &self.part_key,
            &self.order_var_name,
            ts_ini,
            ts_end,
        );

        let mut is_next_page = false;
        // let mut found_invalid_payload = false;
        loop {
            if ts_ini >= ts_end {
                break;
            }
            let result_page =
                Self::fetch_page(query_input.clone(), is_next_page, &self.client).await?;

            let items = result_page.items.ok_or_else(|| "ERROR 120".to_owned())?;
            let items: Vec<T> = from_items(items).map_err(|err| err.to_string())?;
            proc_items(items)?;

            if let Some(key) = result_page.last_evaluated_key {
                is_next_page = true;
                query_input.exclusive_start_key = Some(key);
                continue;
            } else {
                break;
            }
        }
        return Ok(());
    }

    pub fn prepare(&mut self, ts_ini: &str, ts_end: &str) -> Result<(), String> {
        if self.current_query.is_some() {
            return Err(format!("Já existe query em andamento"));
        }
        self.current_query = Some(CurrentQuery {
            query_input: Self::create_query_input(
                &self.table_name,
                &self.key_var_name,
                &self.part_key,
                &self.order_var_name,
                ts_ini,
                ts_end,
            ),
            is_next_page: false,
            acabou: false,
        });
        if ts_ini >= ts_end {
            self.current_query.as_mut().unwrap().acabou = true;
        }
        return Ok(());
    }

    pub async fn next_page(&mut self) -> Result<(Vec<serde_json::Value>, bool), String> {
        let current_query = match &mut self.current_query {
            Some(x) => x,
            None => {
                return Err(format!("Nenhuma query em andamento"));
            }
        };
        if current_query.acabou {
            return Ok((Vec::new(), true));
        }
        let result_page = Self::fetch_page(
            current_query.query_input.clone(),
            current_query.is_next_page,
            &self.client,
        )
        .await?;

        let items = result_page.items.ok_or_else(|| "ERROR 120".to_owned())?;
        let items = from_items(items).map_err(|err| err.to_string())?;

        if let Some(key) = result_page.last_evaluated_key {
            current_query.is_next_page = true;
            current_query.query_input.exclusive_start_key = Some(key);
            current_query.acabou = false;
        } else {
            current_query.acabou = true;
        }

        return Ok((items, current_query.acabou));
    }
}

// loop {
//   if page_ts_ini >= ts_end { break; }
//   let (dynamo_page, next_page_ts_ini) = match fetch_page(&create_query, &page_ts_ini, is_next_page).await {
//     Ok(v) => v,
//     Err(err) => {
//       if err.starts_with("ProvisionedThroughputExceeded:") {
//         provision_error = true;
//         break;
//       } else if err.starts_with("ResourceNotFound:") {
//         println!("Table not found for: {}", dev_id);
//         return Ok(respond_http_json(200, "{}"));
//       } else {
//         return Ok(respond_http_plain_text(400, &format!("ERROR[117] {}", err)));
//       }
//     },
//   };
// };

// match client.query(qparams.clone()).await {
//   Ok(dynamo_page) => {
//     if dynamo_page.items.is_none() {
//       return Err("Query returned no items".to_owned());
//     }
//     let next_page_ts_ini = match &dynamo_page.last_evaluated_key {
//       None => None,
//       Some(last_evaluated_key) => {
//         match last_evaluated_key.get("timestamp") {
//           Some(last_timestamp_a) => {
//             match last_timestamp_a.s {
//               Some(ref last_timestamp_s) => {
//                 Some(String::from(last_timestamp_s) + "A")
//               },
//               None => return Err(String::from("ERROR52")),
//             }
//           },
//           None => return Err(String::from("ERROR53")),
//         }
//       }
//     };
//     return Ok((dynamo_page, next_page_ts_ini));
//   },
//   Err(err1) => {
//     match err1 {
//       RusotoError::Service(ref err2) => {
//         match err2 {
//           QueryError::ProvisionedThroughputExceeded(err_msg) => {
//             if (retries < 2) && (is_next_page) {
//               retries += 1;
//               std::thread::sleep(std::time::Duration::from_millis(2600));
//               continue;
//             } else {
//               return Err(format!("ProvisionedThroughputExceeded: {}", &err_msg));
//             }
//           },
//           QueryError::ResourceNotFound(err_msg) => {
//             // Table not found
//             return Err(format!("ResourceNotFound: {}", &err_msg));
//           },
//           _ => return Err(format!("{}", &err1)),
//         }
//       },
//       _ => return Err(format!("{}", &err1)),
//     };
//   },
// };

// let padronized_tels =
// match padronized_tels {
//   Ok(mut x) => {
//   }
//   Err(e) => {
//       println!("{}", e);
//   }
// };
