use pgorm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .create_table(
                Table::create()
                    .table(Cake::Table)
                    .col(
                        ColumnDef::new(Cake::Id)
                            .integer()
                            .not_null()
                            .auto_increment()
                            .primary_key(),
                    )
                    .col(ColumnDef::new(Cake::Name).string().not_null())
                    .to_owned(),
            )
            .await?;

        manager
            .create_index(
                Index::create()
                    .name("cake_name_index")
                    .table(Cake::Table)
                    .col(Cake::Name)
                    .to_owned(),
            )
            .await?;
        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .drop_table(Table::drop().table(Cake::Table).to_owned())
            .await?;

        if std::env::var_os("ABORT_MIGRATION").eq(&Some("YES".into())) {
            return Err(DbErr::Migration(
                "Abort migration and rollback changes".into(),
            ));
        }

        Ok(())
    }
}

#[derive(DeriveIden)]
pub enum Cake {
    Table,
    Id,
    Name,
}
