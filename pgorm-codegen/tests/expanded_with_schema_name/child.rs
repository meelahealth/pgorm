use pgorm::entity::prelude::*;

#[derive(Copy, Clone, Default, Debug, DeriveEntity)]
pub struct Entity;

impl EntityName for Entity {
    fn schema_name(&self) -> Option< &str > {
        Some("schema_name")
    }

    fn table_name(&self) -> &str {
        "child"
    }
}

#[derive(Clone, Debug, PartialEq, DeriveModel, DeriveActiveModel, Eq)]
pub struct Model {
    pub id: i32,
    pub parent_id1: i32,
    pub parent_id2: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveColumn)]
pub enum Column {
    Id,
    ParentId1,
    ParentId2,
}

#[derive(Copy, Clone, Debug, EnumIter, DerivePrimaryKey)]
pub enum PrimaryKey {
    Id,
}

impl PrimaryKeyTrait for PrimaryKey {
    type ValueType = i32;
    fn auto_increment() -> bool {
        true
    }
}

#[derive(Copy, Clone, Debug, EnumIter)]
pub enum Relation {
    Parent,
}

impl ColumnTrait for Column {
    type EntityName = Entity;
    fn def(&self) -> ColumnDef {
        match self {
            Self::Id => ColumnType::Integer.def(),
            Self::ParentId1 => ColumnType::Integer.def(),
            Self::ParentId2 => ColumnType::Integer.def(),
        }
    }
}

impl RelationTrait for Relation {
    fn def(&self) -> RelationDef {
        match self {
            Self::Parent => Entity::belongs_to(super::parent::Entity)
                .from((Column::ParentId1, Column::ParentId2))
                .to((super::parent::Column::Id1, super::parent::Column::Id2))
                .into(),
        }
    }
}

impl Related<super::parent::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Parent.def()
    }
}

impl ActiveModelBehavior for ActiveModel {}
