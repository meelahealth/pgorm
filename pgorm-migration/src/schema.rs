//! > Adapted from https://github.com/loco-rs/loco/blob/master/src/schema.rs
//!
//! # Database Table Schema Helpers
//!
//! This module defines functions and helpers for creating database table
//! schemas using the `pgorm` and `sea-query` libraries.
//!
//! # Example
//!
//! The following example shows how the user migration file should be and using
//! the schema helpers to create the Db fields.
//!
//! ```rust
//! use pgorm_migration::{prelude::*, schema::*};
//!
//! #[derive(DeriveMigrationName)]
//! pub struct Migration;
//!
//! #[async_trait::async_trait]
//! impl MigrationTrait for Migration {
//!     async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
//!         let table = table_auto(Users::Table)
//!             .col(pk_auto(Users::Id))
//!             .col(uuid(Users::Pid))
//!             .col(string_uniq(Users::Email))
//!             .col(string(Users::Password))
//!             .col(string(Users::Name))
//!             .col(string_null(Users::ResetToken))
//!             .col(timestamp_null(Users::ResetSentAt))
//!             .to_owned();
//!         manager.create_table(table).await?;
//!         Ok(())
//!     }
//!
//!     async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
//!         manager
//!             .drop_table(Table::drop().table(Users::Table).to_owned())
//!             .await
//!     }
//! }
//!
//! #[derive(Iden)]
//! pub enum Users {
//!     Table,
//!     Id,
//!     Pid,
//!     Email,
//!     Name,
//!     Password,
//!     ResetToken,
//!     ResetSentAt,
//! }
//! ```

use crate::{prelude::Iden, pgorm_query};
use pgorm::pgorm_query::{
    Alias, ColumnDef, ColumnType, Expr, IntoIden, PgInterval, Table, TableCreateStatement,
};

#[derive(Iden)]
enum GeneralIds {
    CreatedAt,
    UpdatedAt,
}

/// Wrapping table schema creation.
pub fn table_auto<T: IntoIden + 'static>(name: T) -> TableCreateStatement {
    timestamps(Table::create().table(name).if_not_exists().take())
}

/// Create a primary key column with auto-increment feature.
pub fn pk_auto<T: IntoIden>(name: T) -> ColumnDef {
    integer(name).auto_increment().primary_key().take()
}

pub fn char_len<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    char_len_null(col, length).not_null().take()
}

pub fn char_len_null<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    ColumnDef::new(col).char_len(length).take()
}

pub fn char_len_uniq<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    char_len(col, length).unique_key().take()
}

pub fn char<T: IntoIden>(col: T) -> ColumnDef {
    char_null(col).not_null().take()
}

pub fn char_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).char().take()
}

pub fn char_uniq<T: IntoIden>(col: T) -> ColumnDef {
    char(col).unique_key().take()
}

pub fn string_len<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    string_len_null(col, length).not_null().take()
}

pub fn string_len_null<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    ColumnDef::new(col).string_len(length).take()
}

pub fn string_len_uniq<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    string_len(col, length).unique_key().take()
}

pub fn string<T: IntoIden>(col: T) -> ColumnDef {
    string_null(col).not_null().take()
}

pub fn string_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).string().take()
}

pub fn string_uniq<T: IntoIden>(col: T) -> ColumnDef {
    string(col).unique_key().take()
}

pub fn text<T: IntoIden>(col: T) -> ColumnDef {
    text_null(col).not_null().take()
}

pub fn text_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).text().take()
}

pub fn text_uniq<T: IntoIden>(col: T) -> ColumnDef {
    text(col).unique_key().take()
}

pub fn tiny_integer<T: IntoIden>(col: T) -> ColumnDef {
    tiny_integer_null(col).not_null().take()
}

pub fn tiny_integer_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).tiny_integer().take()
}

pub fn tiny_integer_uniq<T: IntoIden>(col: T) -> ColumnDef {
    tiny_integer(col).unique_key().take()
}

pub fn small_integer<T: IntoIden>(col: T) -> ColumnDef {
    small_integer_null(col).not_null().take()
}

pub fn small_integer_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).small_integer().take()
}

pub fn small_integer_uniq<T: IntoIden>(col: T) -> ColumnDef {
    small_integer(col).unique_key().take()
}

pub fn integer<T: IntoIden>(col: T) -> ColumnDef {
    integer_null(col).not_null().take()
}

pub fn integer_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).integer().take()
}

pub fn integer_uniq<T: IntoIden>(col: T) -> ColumnDef {
    integer(col).unique_key().take()
}

pub fn big_integer<T: IntoIden>(col: T) -> ColumnDef {
    big_integer_null(col).not_null().take()
}

pub fn big_integer_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).big_integer().take()
}

pub fn big_integer_uniq<T: IntoIden>(col: T) -> ColumnDef {
    big_integer(col).unique_key().take()
}

pub fn tiny_unsigned<T: IntoIden>(col: T) -> ColumnDef {
    tiny_unsigned_null(col).not_null().take()
}

pub fn tiny_unsigned_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).tiny_unsigned().take()
}

pub fn tiny_unsigned_uniq<T: IntoIden>(col: T) -> ColumnDef {
    tiny_unsigned(col).unique_key().take()
}

pub fn small_unsigned<T: IntoIden>(col: T) -> ColumnDef {
    small_unsigned_null(col).not_null().take()
}

pub fn small_unsigned_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).small_unsigned().take()
}

pub fn small_unsigned_uniq<T: IntoIden>(col: T) -> ColumnDef {
    small_unsigned(col).unique_key().take()
}

pub fn unsigned<T: IntoIden>(col: T) -> ColumnDef {
    unsigned_null(col).not_null().take()
}

pub fn unsigned_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).unsigned().take()
}

pub fn unsigned_uniq<T: IntoIden>(col: T) -> ColumnDef {
    unsigned(col).unique_key().take()
}

pub fn big_unsigned<T: IntoIden>(col: T) -> ColumnDef {
    big_unsigned_null(col).not_null().take()
}

pub fn big_unsigned_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).big_unsigned().take()
}

pub fn big_unsigned_uniq<T: IntoIden>(col: T) -> ColumnDef {
    big_unsigned(col).unique_key().take()
}

pub fn float<T: IntoIden>(col: T) -> ColumnDef {
    float_null(col).not_null().take()
}

pub fn float_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).float().take()
}

pub fn float_uniq<T: IntoIden>(col: T) -> ColumnDef {
    float(col).unique_key().take()
}

pub fn double<T: IntoIden>(col: T) -> ColumnDef {
    double_null(col).not_null().take()
}

pub fn double_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).double().take()
}

pub fn double_uniq<T: IntoIden>(col: T) -> ColumnDef {
    double(col).unique_key().take()
}

pub fn decimal_len<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    decimal_len_null(col, precision, scale).not_null().take()
}

pub fn decimal_len_null<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    ColumnDef::new(col).decimal_len(precision, scale).take()
}

pub fn decimal_len_uniq<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    decimal_len(col, precision, scale).unique_key().take()
}

pub fn decimal<T: IntoIden>(col: T) -> ColumnDef {
    decimal_null(col).not_null().take()
}

pub fn decimal_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).decimal().take()
}

pub fn decimal_uniq<T: IntoIden>(col: T) -> ColumnDef {
    decimal(col).unique_key().take()
}

pub fn date_time<T: IntoIden>(col: T) -> ColumnDef {
    date_time_null(col).not_null().take()
}

pub fn date_time_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).date_time().take()
}

pub fn date_time_uniq<T: IntoIden>(col: T) -> ColumnDef {
    date_time(col).unique_key().take()
}

pub fn interval<T: IntoIden>(
    col: T,
    fields: Option<PgInterval>,
    precision: Option<u32>,
) -> ColumnDef {
    interval_null(col, fields, precision).not_null().take()
}

pub fn interval_null<T: IntoIden>(
    col: T,
    fields: Option<PgInterval>,
    precision: Option<u32>,
) -> ColumnDef {
    ColumnDef::new(col).interval(fields, precision).take()
}

pub fn interval_uniq<T: IntoIden>(
    col: T,
    fields: Option<PgInterval>,
    precision: Option<u32>,
) -> ColumnDef {
    interval(col, fields, precision).unique_key().take()
}

pub fn timestamp<T: IntoIden>(col: T) -> ColumnDef {
    timestamp_null(col).not_null().take()
}

pub fn timestamp_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).timestamp().take()
}

pub fn timestamp_uniq<T: IntoIden>(col: T) -> ColumnDef {
    timestamp(col).unique_key().take()
}

pub fn timestamp_with_time_zone<T: IntoIden>(col: T) -> ColumnDef {
    timestamp_with_time_zone_null(col).not_null().take()
}

pub fn timestamp_with_time_zone_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).timestamp_with_time_zone().take()
}

pub fn timestamp_with_time_zone_uniq<T: IntoIden>(col: T) -> ColumnDef {
    timestamp_with_time_zone(col).unique_key().take()
}

pub fn time<T: IntoIden>(col: T) -> ColumnDef {
    time_null(col).not_null().take()
}

pub fn time_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).time().take()
}

pub fn time_uniq<T: IntoIden>(col: T) -> ColumnDef {
    time(col).unique_key().take()
}

pub fn date<T: IntoIden>(col: T) -> ColumnDef {
    date_null(col).not_null().take()
}

pub fn date_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).date().take()
}

pub fn date_uniq<T: IntoIden>(col: T) -> ColumnDef {
    date(col).unique_key().take()
}

pub fn year<T: IntoIden>(col: T) -> ColumnDef {
    year_null(col).not_null().take()
}

pub fn year_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).year().take()
}

pub fn year_uniq<T: IntoIden>(col: T) -> ColumnDef {
    year(col).unique_key().take()
}

pub fn binary_len<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    binary_len_null(col, length).not_null().take()
}

pub fn binary_len_null<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    ColumnDef::new(col).binary_len(length).take()
}

pub fn binary_len_uniq<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    binary_len(col, length).unique_key().take()
}

pub fn binary<T: IntoIden>(col: T) -> ColumnDef {
    binary_null(col).not_null().take()
}

pub fn binary_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).binary().take()
}

pub fn binary_uniq<T: IntoIden>(col: T) -> ColumnDef {
    binary(col).unique_key().take()
}

pub fn var_binary<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    var_binary_null(col, length).not_null().take()
}

pub fn var_binary_null<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    ColumnDef::new(col).var_binary(length).take()
}

pub fn var_binary_uniq<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    var_binary(col, length).unique_key().take()
}

pub fn bit<T: IntoIden>(col: T, length: Option<u32>) -> ColumnDef {
    bit_null(col, length).not_null().take()
}

pub fn bit_null<T: IntoIden>(col: T, length: Option<u32>) -> ColumnDef {
    ColumnDef::new(col).bit(length).take()
}

pub fn bit_uniq<T: IntoIden>(col: T, length: Option<u32>) -> ColumnDef {
    bit(col, length).unique_key().take()
}

pub fn varbit<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    varbit_null(col, length).not_null().take()
}

pub fn varbit_null<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    ColumnDef::new(col).varbit(length).take()
}

pub fn varbit_uniq<T: IntoIden>(col: T, length: u32) -> ColumnDef {
    varbit(col, length).unique_key().take()
}

pub fn blob<T: IntoIden>(col: T) -> ColumnDef {
    blob_null(col).not_null().take()
}

pub fn blob_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).blob().take()
}

pub fn blob_uniq<T: IntoIden>(col: T) -> ColumnDef {
    blob(col).unique_key().take()
}

pub fn boolean<T: IntoIden>(col: T) -> ColumnDef {
    boolean_null(col).not_null().take()
}

pub fn boolean_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).boolean().take()
}

pub fn boolean_uniq<T: IntoIden>(col: T) -> ColumnDef {
    boolean(col).unique_key().take()
}

pub fn money_len<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    money_len_null(col, precision, scale).not_null().take()
}

pub fn money_len_null<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    ColumnDef::new(col).money_len(precision, scale).take()
}

pub fn money_len_uniq<T: IntoIden>(col: T, precision: u32, scale: u32) -> ColumnDef {
    money_len(col, precision, scale).unique_key().take()
}

pub fn money<T: IntoIden>(col: T) -> ColumnDef {
    money_null(col).not_null().take()
}

pub fn money_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).money().take()
}

pub fn money_uniq<T: IntoIden>(col: T) -> ColumnDef {
    money(col).unique_key().take()
}

pub fn json<T: IntoIden>(col: T) -> ColumnDef {
    json_null(col).not_null().take()
}

pub fn json_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).json().take()
}

pub fn json_uniq<T: IntoIden>(col: T) -> ColumnDef {
    json(col).unique_key().take()
}

pub fn json_binary<T: IntoIden>(col: T) -> ColumnDef {
    json_binary_null(col).not_null().take()
}

pub fn json_binary_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).json_binary().take()
}

pub fn json_binary_uniq<T: IntoIden>(col: T) -> ColumnDef {
    json_binary(col).unique_key().take()
}

pub fn uuid<T: IntoIden>(col: T) -> ColumnDef {
    uuid_null(col).not_null().take()
}

pub fn uuid_null<T: IntoIden>(col: T) -> ColumnDef {
    ColumnDef::new(col).uuid().take()
}

pub fn uuid_uniq<T: IntoIden>(col: T) -> ColumnDef {
    uuid(col).unique_key().take()
}

pub fn custom<T: IntoIden>(col: T, name: T) -> ColumnDef {
    custom_null(col, name).not_null().take()
}

pub fn custom_null<T: IntoIden>(col: T, name: T) -> ColumnDef {
    ColumnDef::new(col).custom(name).take()
}

pub fn enumeration<T, N, S, V>(col: T, name: N, variants: V) -> ColumnDef
where
    T: IntoIden,
    N: IntoIden,
    S: IntoIden,
    V: IntoIterator<Item = S>,
{
    enumeration_null(col, name, variants).not_null().take()
}

pub fn enumeration_null<T, N, S, V>(col: T, name: N, variants: V) -> ColumnDef
where
    T: IntoIden,
    N: IntoIden,
    S: IntoIden,
    V: IntoIterator<Item = S>,
{
    ColumnDef::new(col).enumeration(name, variants).take()
}

pub fn enumeration_uniq<T, N, S, V>(col: T, name: N, variants: V) -> ColumnDef
where
    T: IntoIden,
    N: IntoIden,
    S: IntoIden,
    V: IntoIterator<Item = S>,
{
    enumeration(col, name, variants).unique_key().take()
}

pub fn array<T: IntoIden>(col: T, elem_type: ColumnType) -> ColumnDef {
    array_null(col, elem_type).not_null().take()
}

pub fn array_null<T: IntoIden>(col: T, elem_type: ColumnType) -> ColumnDef {
    ColumnDef::new(col).array(elem_type).take()
}

pub fn array_uniq<T: IntoIden>(col: T, elem_type: ColumnType) -> ColumnDef {
    array(col, elem_type).unique_key().take()
}

/// Add timestamp columns (`CreatedAt` and `UpdatedAt`) to an existing table.
// pub fn timestamps(t: TableCreateStatement) -> TableCreateStatement {
//     let mut t = t;
//     t.col(timestamp(GeneralIds::CreatedAt).default(Expr::current_timestamp()))
//         .col(timestamp(GeneralIds::UpdatedAt).default(Expr::current_timestamp()))
//         .take()
// }

/// Create an Alias.
pub fn name<T: Into<String>>(name: T) -> Alias {
    Alias::new(name)
}
