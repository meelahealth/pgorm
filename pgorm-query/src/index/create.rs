use inherent::inherent;

use crate::{QueryBuilder, SchemaStatementBuilder, types::*};

use super::common::*;

/// Create an index for an existing table
///
/// # Examples
///
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col(Glyph::Aspect)
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE INDEX "idx-glyph-aspect" ON "glyph" ("aspect")"#
/// );
/// ```
/// Create index if not exists
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .if_not_exists()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col(Glyph::Aspect)
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE INDEX IF NOT EXISTS "idx-glyph-aspect" ON "glyph" ("aspect")"#
/// );
/// ```
/// Index with prefix
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col((Glyph::Aspect, 128))
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE INDEX "idx-glyph-aspect" ON "glyph" ("aspect" (128))"#
/// );
/// ```
/// Index with order
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col((Glyph::Aspect, IndexOrder::Desc))
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE INDEX "idx-glyph-aspect" ON "glyph" ("aspect" DESC)"#
/// );
/// ```
/// Index on multi-columns
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col((Glyph::Image, IndexOrder::Asc))
///     .col((Glyph::Aspect, IndexOrder::Desc))
///     .unique()
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE UNIQUE INDEX "idx-glyph-aspect" ON "glyph" ("image" ASC, "aspect" DESC)"#
/// );
/// ```
/// Index with prefix and order
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let index = Index::create()
///     .name("idx-glyph-aspect")
///     .table(Glyph::Table)
///     .col((Glyph::Aspect, 64, IndexOrder::Asc))
///     .to_owned();
///
/// assert_eq!(
///     index.to_string(QueryBuilder),
///     r#"CREATE INDEX "idx-glyph-aspect" ON "glyph" ("aspect" (64) ASC)"#
/// );
/// ```
#[derive(Default, Debug, Clone)]
pub struct IndexCreateStatement {
    pub(crate) table: Option<TableRef>,
    pub(crate) index: TableIndex,
    pub(crate) primary: bool,
    pub(crate) unique: bool,
    pub(crate) nulls_not_distinct: bool,
    pub(crate) index_type: Option<IndexType>,
    pub(crate) if_not_exists: bool,
}

/// Specification of a table index
#[derive(Debug, Clone)]
pub enum IndexType {
    BTree,
    FullText,
    Hash,
    Custom(DynIden),
}

impl IndexCreateStatement {
    /// Construct a new [`IndexCreateStatement`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create index if index not exists
    pub fn if_not_exists(&mut self) -> &mut Self {
        self.if_not_exists = true;
        self
    }

    /// Set index name
    pub fn name<T>(&mut self, name: T) -> &mut Self
    where
        T: Into<String>,
    {
        self.index.name(name);
        self
    }

    /// Set target table
    pub fn table<T>(&mut self, table: T) -> &mut Self
    where
        T: IntoTableRef,
    {
        self.table = Some(table.into_table_ref());
        self
    }

    /// Add index column
    pub fn col<C>(&mut self, col: C) -> &mut Self
    where
        C: IntoIndexColumn,
    {
        self.index.col(col.into_index_column());
        self
    }

    /// Set index as primary
    pub fn primary(&mut self) -> &mut Self {
        self.primary = true;
        self
    }

    /// Set index as unique
    pub fn unique(&mut self) -> &mut Self {
        self.unique = true;
        self
    }

    /// Set nulls to not be treated as distinct values. Only available on Postgres.
    pub fn nulls_not_distinct(&mut self) -> &mut Self {
        self.nulls_not_distinct = true;
        self
    }

    /// Set index as full text.
    /// On MySQL, this is `FULLTEXT`.
    /// On PgSQL, this is `GIN`.
    pub fn full_text(&mut self) -> &mut Self {
        self.index_type(IndexType::FullText)
    }

    /// Set index type. Not available on Sqlite.
    pub fn index_type(&mut self, index_type: IndexType) -> &mut Self {
        self.index_type = Some(index_type);
        self
    }

    pub fn is_primary_key(&self) -> bool {
        self.primary
    }

    pub fn is_unique_key(&self) -> bool {
        self.unique
    }

    pub fn is_nulls_not_distinct(&self) -> bool {
        self.nulls_not_distinct
    }

    pub fn get_index_spec(&self) -> &TableIndex {
        &self.index
    }

    pub fn take(&mut self) -> Self {
        Self {
            table: self.table.take(),
            index: self.index.take(),
            primary: self.primary,
            unique: self.unique,
            nulls_not_distinct: self.nulls_not_distinct,
            index_type: self.index_type.take(),
            if_not_exists: self.if_not_exists,
        }
    }
}

#[inherent]
impl SchemaStatementBuilder for IndexCreateStatement {
    pub fn build(&self, schema_builder: QueryBuilder) -> String {
        let mut sql = String::with_capacity(256);
        schema_builder.prepare_index_create_statement(self, &mut sql);
        sql
    }

    pub fn build_any(&self, schema_builder: &QueryBuilder) -> String {
        let mut sql = String::with_capacity(256);
        schema_builder.prepare_index_create_statement(self, &mut sql);
        sql
    }

    pub fn to_string(&self, schema_builder: QueryBuilder) -> String;
}
