use crate::GlobalVars;
use rusoto_core::RusotoError;
use rusoto_dynamodb::{
    AttributeDefinition, CreateTableInput, CreateTableOutput, DynamoDbClient, KeySchemaElement,
    PutItemError, PutItemOutput,
};
use rusoto_dynamodb::{DynamoDb, PutItemInput};
use std::sync::Arc;

type OnTableNotFound = dyn Fn(&Arc<GlobalVars>, &str) + Send + Sync;

pub struct AWSConfig {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

pub struct DynamoDBClientDiel {
    pub client: DynamoDbClient,
    // pub on_inserted: Option<&'static OnInserted>,
    pub on_table_not_found: Option<&'static OnTableNotFound>, // on_table_not_found(globs, proposed_table_name)
}

impl DynamoDBClientDiel {
    pub fn new(
        config: &AWSConfig,
        on_table_not_found: &'static OnTableNotFound,
    ) -> DynamoDBClientDiel {
        DynamoDBClientDiel {
            client: DynamoDBClientDiel::create_client(config),
            // on_inserted: None,
            on_table_not_found: Some(on_table_not_found),
        }
    }

    pub fn create_client(config: &AWSConfig) -> DynamoDbClient {
        std::env::set_var("AWS_ACCESS_KEY_ID", &config.access_key_id);
        std::env::set_var("AWS_SECRET_ACCESS_KEY", &config.secret_access_key);
        if let Some(session_token) = &config.session_token {
            std::env::set_var("AWS_SESSION_TOKEN", session_token);
        }
        // std::env::set_var("AWS_DEFAULT_REGION", &config.default_region); // "us-east-1"

        DynamoDbClient::new(rusoto_core::Region::UsEast1)
    }

    pub async fn insert_telemetry(
        &self,
        table_name: &str,
        payload: &serde_json::Value,
        globs: &Arc<GlobalVars>,
    ) -> Result<PutItemOutput, String> {
        let req_params = PutItemInput {
            table_name: table_name.to_owned(),
            item: serde_dynamo::to_item(&payload).unwrap(),
            ..PutItemInput::default()
        };
        let result = self.client.put_item(req_params).await;
        if let Err(RusotoError::Service(PutItemError::ResourceNotFound(_))) = &result {
            if let Some(on_table_not_found) = &self.on_table_not_found {
                on_table_not_found(globs, table_name);
            }
        }
        result.map_err(|err| format!("{:?}", err))
    }

    pub async fn create_new_table(&self, table_name: String) -> Result<CreateTableOutput, String> {
        // let client = DynamoDbClient::new(rusoto_core::Region::UsEast1);
        let response = self
            .client
            .create_table(CreateTableInput {
                table_name: table_name,
                attribute_definitions: vec![
                    AttributeDefinition {
                        attribute_name: "dev_id".to_owned(),
                        attribute_type: "S".to_owned(),
                    },
                    AttributeDefinition {
                        attribute_name: "timestamp".to_owned(),
                        attribute_type: "S".to_owned(),
                    },
                ],
                key_schema: vec![
                    KeySchemaElement {
                        attribute_name: "dev_id".to_owned(),
                        key_type: "HASH".to_owned(),
                    },
                    KeySchemaElement {
                        attribute_name: "timestamp".to_owned(),
                        key_type: "RANGE".to_owned(),
                    },
                ],
                billing_mode: Some("PAY_PER_REQUEST".to_owned()),
                // BillingMode: 'PROVISIONED',
                // ProvisionedThroughput: {
                //   ReadCapacityUnits: 5,
                //   WriteCapacityUnits: 5,
                // },
                ..Default::default()
            })
            .await
            .map_err(|err| format!("[102] {err}"))?;

        // typedWarn('DYNAMODB_TABLE_CREATION', `Tabela do DynamoDB criada com sucesso: ${proposedTableName}`);
        Ok(response)
    }
}
