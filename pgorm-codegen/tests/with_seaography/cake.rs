use pgorm::entity::prelude:: * ;

#[derive(Clone, Debug, PartialEq, DeriveEntityModel, Eq)]
#[pgorm(table_name = "cake")]
pub struct Model {
    #[pgorm(primary_key)]
    pub id: i32,
    #[pgorm(column_type = "Text", nullable)]
    pub name: Option<String> ,
    pub base_id: Option<i32> ,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {
    #[pgorm(has_many = "super::fruit::Entity")]
    Fruit,
    #[pgorm(has_one = "Entity")]
    SelfRef ,
}

impl Related<super::fruit::Entity> for Entity {
    fn to() -> RelationDef {
        Relation::Fruit.def()
    }
}

impl Related<super::filling::Entity> for Entity {
    fn to() -> RelationDef {
        super::cake_filling::Relation::Filling.def()
    }
    fn via() -> Option<RelationDef> {
        Some(super::cake_filling::Relation::Cake.def().rev())
    }
}

impl ActiveModelBehavior for ActiveModel {}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelatedEntity)]
pub enum RelatedEntity {
    #[pgorm(entity = "super::fruit::Entity")]
    Fruit,
    #[pgorm(entity = "Entity", def = "Relation::SelfRef.def()")]
    SelfRef,
    #[pgorm(entity = "Entity", def = "Relation::SelfRef.def().rev()")]
    SelfRefReverse,
    #[pgorm(entity = "super::filling::Entity")]
    Filling
}
