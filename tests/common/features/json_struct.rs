use pgorm::FromJsonQueryResult;
use pgorm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[pgorm(table_name = "json_struct")]
pub struct Model {
    #[pgorm(primary_key)]
    pub id: i32,
    pub json: Json,
    pub json_value: KeyValue,
    pub json_value_opt: Option<KeyValue>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, FromJsonQueryResult)]
pub struct KeyValue {
    pub id: i32,
    pub name: String,
    pub price: f32,
    pub notes: Option<String>,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}
