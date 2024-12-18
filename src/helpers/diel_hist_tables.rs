use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct CustomTableRule {
    pub topic: String,  // "data/dac/#"
    pub prop: String,   // "dev_id"
    pub prefix: String, // "DAC40121"
    pub table: String,  // "DAC40121XXXX_RAW"
}

#[derive(Serialize, Deserialize)]
pub struct PrefixAndTable {
    pub dev_prefix: String,
    pub table_name: String,
}

pub struct TopicRule {
    pub topic_subscribe: String,
    pub topic_exact: String,
    pub topic_begin: Option<String>,
    pub topic_props: Vec<PropRule>,
}

pub struct PropRule {
    pub prop_name: String,
    pub tables_list: Vec<(String, String)>,
}

pub type TablesConfig = Vec<TopicRule>;

pub trait TopicRulesUtils<'a> {
    fn find_matching_topic_rule(&'a self, topic: &str) -> Option<&'a TopicRule>;
}

impl<'a> TopicRulesUtils<'a> for TablesConfig {
    fn find_matching_topic_rule(&'a self, topic: &str) -> Option<&'a TopicRule> {
        for rule in self {
            if topic == rule.topic_exact {
                return Some(rule);
            }
            if let Some(topic_begin) = &rule.topic_begin {
                if topic.starts_with(topic_begin) {
                    return Some(rule);
                }
            }
        }
        return None;
    }
}

pub fn load_tables(custom_table_rules: &Vec<CustomTableRule>) -> Result<TablesConfig, String> {
    let mut tables: TablesConfig = Vec::new();

    for row in custom_table_rules {
        add_table_item(&mut tables, &row.topic, &row.prop, &row.prefix, &row.table);
    }

    return Ok(tables);
}

fn add_table_item(tables: &mut Vec<TopicRule>, topic: &str, prop: &str, prefix: &str, table: &str) {
    let topic_item = tables.iter_mut().find(|x| x.topic_subscribe == topic);
    let topic_item = match topic_item {
        Some(v) => v,
        None => {
            let (topic_exact, topic_begin) = if topic.ends_with("/#") {
                (
                    topic[..topic.len() - 2].to_owned(),
                    Some(topic[..topic.len() - 1].to_owned()),
                )
            } else {
                (topic.to_owned(), None)
            };
            tables.push(TopicRule {
                topic_subscribe: topic.to_owned(),
                topic_exact,
                topic_begin,
                topic_props: Vec::new(),
            });
            let last_i = tables.len() - 1;
            &mut tables[last_i]
        }
    };

    let prop_item = topic_item
        .topic_props
        .iter_mut()
        .find(|x| x.prop_name == prop);
    let prop_item = match prop_item {
        Some(v) => v,
        None => {
            topic_item.topic_props.push(PropRule {
                prop_name: prop.to_owned(),
                tables_list: Vec::new(),
            });
            let last_i = topic_item.topic_props.len() - 1;
            &mut topic_item.topic_props[last_i]
        }
    };

    let table_item = prop_item.tables_list.iter_mut().find(|x| x.0 == prefix);
    match table_item {
        Some(_v) => { /* já existe */ }
        None => {
            prop_item
                .tables_list
                .push((prefix.to_owned(), table.to_owned()));
        }
    };
}

pub enum BigQueryHistoryTable {
    None,                // Do not save to BigQuery
    SingleTable(String), // Save all telemetries to one table
    DevType,             // Use tables like "dac_telemetry"
    DevGeneration,       // Use tables like "DAC40123_telemetry"
    DevId,               // Each device has its own table
}
