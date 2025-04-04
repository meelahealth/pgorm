use inherent::inherent;

use crate::{ForeignKeyAction, QueryBuilder, SchemaStatementBuilder, TableForeignKey, types::*};

/// Create a foreign key constraint for an existing table. Unsupported by Sqlite
///
/// # Examples
///
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let foreign_key = ForeignKey::create()
///     .name("FK_character_font")
///     .from(Char::Table, Char::FontId)
///     .to(Font::Table, Font::Id)
///     .to_owned();
///
/// assert_eq!(
///     foreign_key.to_string(QueryBuilder),
///     [
///         r#"ALTER TABLE "character" ADD CONSTRAINT "FK_character_font""#,
///         r#"FOREIGN KEY ("font_id") REFERENCES "font" ("id")"#,
///     ]
///     .join(" ")
/// );
/// ```
///
/// Composite key
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let foreign_key = ForeignKey::create()
///     .name("FK_character_glyph")
///     .from(Char::Table, (Char::FontId, Char::Id))
///     .to(Glyph::Table, (Char::FontId, Glyph::Id))
///     .to_owned();
///
/// assert_eq!(
///     foreign_key.to_string(QueryBuilder),
///     [
///         r#"ALTER TABLE "character" ADD CONSTRAINT "FK_character_glyph""#,
///         r#"FOREIGN KEY ("font_id", "id") REFERENCES "glyph" ("font_id", "id")"#,
///     ]
///     .join(" ")
/// );
/// ```
///
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let foreign_key = ForeignKey::create()
///     .name("FK_character_id")
///     .from(Character::Table, Character::Id)
///     .to(Character::Table, Character::Id)
///     .to_owned();
///
/// assert_eq!(
///     foreign_key.to_string(QueryBuilder),
///     r#"ALTER TABLE "character" ADD CONSTRAINT "FK_character_id" FOREIGN KEY ("id") REFERENCES "character" ("id")"#
/// );
/// ```
///
/// ```
/// use pgorm_query::{*, tests_cfg::*};
///
/// let foreign_key = ForeignKey::create()
///     .name("FK_character_id")
///     .from(Character::Table, Character::Id)
///     .to(Character::Table, Character::Id)
///     .on_delete(ForeignKeyAction::Cascade)
///     .on_update(ForeignKeyAction::Cascade)
///     .to_owned();
///
/// assert_eq!(
///     foreign_key.to_string(QueryBuilder),
///     r#"ALTER TABLE "character" ADD CONSTRAINT "FK_character_id" FOREIGN KEY ("id") REFERENCES "character" ("id") ON DELETE CASCADE ON UPDATE CASCADE"#
/// );
/// ```
#[derive(Default, Debug, Clone)]
pub struct ForeignKeyCreateStatement {
    pub(crate) foreign_key: TableForeignKey,
}

impl ForeignKeyCreateStatement {
    /// Construct a new [`ForeignKeyCreateStatement`]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set foreign key name
    pub fn name<T>(&mut self, name: T) -> &mut Self
    where
        T: Into<String>,
    {
        self.foreign_key.name(name);
        self
    }

    /// Set key table and columns
    pub fn from<T, C>(&mut self, table: T, columns: C) -> &mut Self
    where
        T: IntoTableRef,
        C: IdenList,
    {
        self.foreign_key.from_tbl(table);
        for col in columns.into_iter() {
            self.foreign_key.from_col(col);
        }
        self
    }

    /// Set referencing table and columns
    pub fn to<T, C>(&mut self, table: T, columns: C) -> &mut Self
    where
        T: IntoTableRef,
        C: IdenList,
    {
        self.foreign_key.to_tbl(table);
        for col in columns.into_iter() {
            self.foreign_key.to_col(col);
        }
        self
    }

    /// Set key table
    pub fn from_tbl<T>(&mut self, table: T) -> &mut Self
    where
        T: IntoTableRef,
    {
        self.foreign_key.from_tbl(table);
        self
    }

    /// Set referencing table
    pub fn to_tbl<R>(&mut self, ref_table: R) -> &mut Self
    where
        R: IntoTableRef,
    {
        self.foreign_key.to_tbl(ref_table);
        self
    }

    /// Add key column
    pub fn from_col<T>(&mut self, column: T) -> &mut Self
    where
        T: IntoIden,
    {
        self.foreign_key.from_col(column);
        self
    }

    /// Add referencing column
    pub fn to_col<R>(&mut self, ref_column: R) -> &mut Self
    where
        R: IntoIden,
    {
        self.foreign_key.to_col(ref_column);
        self
    }

    /// Set on delete action
    pub fn on_delete(&mut self, action: ForeignKeyAction) -> &mut Self {
        self.foreign_key.on_delete(action);
        self
    }

    /// Set on update action
    pub fn on_update(&mut self, action: ForeignKeyAction) -> &mut Self {
        self.foreign_key.on_update(action);
        self
    }

    pub fn get_foreign_key(&self) -> &TableForeignKey {
        &self.foreign_key
    }

    pub fn take(&mut self) -> Self {
        Self {
            foreign_key: self.foreign_key.take(),
        }
    }
}

#[inherent]
impl SchemaStatementBuilder for ForeignKeyCreateStatement {
    pub fn build(&self, schema_builder: QueryBuilder) -> String {
        let mut sql = String::with_capacity(256);
        schema_builder.prepare_foreign_key_create_statement(self, &mut sql);
        sql
    }

    pub fn build_any(&self, schema_builder: &QueryBuilder) -> String {
        let mut sql = String::with_capacity(256);
        schema_builder.prepare_foreign_key_create_statement(self, &mut sql);
        sql
    }

    pub fn to_string(&self, schema_builder: QueryBuilder) -> String;
}
