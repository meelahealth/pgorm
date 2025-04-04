use pgorm::entity::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Serialize, Deserialize)]
#[pgorm(table_name = "underscores")]
pub struct Model {
    #[pgorm(primary_key)]
    pub id: u32,
    #[pgorm(column_name = "a_b_c_d")]
    pub a_b_c_d: i32,
    #[pgorm(column_name = "a_b_c_dd")]
    pub a_b_c_dd: i32,
    #[pgorm(column_name = "a_b_cc_d")]
    pub a_b_cc_d: i32,
    #[pgorm(column_name = "a_bb_c_d")]
    pub a_bb_c_d: i32,
    #[pgorm(column_name = "aa_b_c_d")]
    pub aa_b_c_d: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

#[cfg(test)]
mod tests {
    use super::*;
    use pgorm::Iterable;

    #[test]
    fn column_names() {
        assert_eq!(
            Column::iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            vec!["id", "a_b_c_d", "a_b_c_dd", "a_b_cc_d", "a_bb_c_d", "aa_b_c_d"]
        )
    }
}
