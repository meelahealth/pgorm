mod connection;
mod db_connection;
// mod statement;
// mod stream;
// mod transaction;

pub use connection::*;
pub use db_connection::*;
pub use tokio_postgres::Config;

use pgorm_pool::{Manager, ManagerConfig, Pool, PoolBuilder, RecyclingMethod};
use std::{
    collections::BTreeMap,
    sync::Arc,
};
// pub use statement::*;
// pub use stream::*;
// pub use transaction::*;

/// Method to create a [DatabasePool] on a database
pub fn connect<T>(config: Config, tls: T) -> DatabasePool
    where
        T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,

{
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
        tag: None,
    };
    let mgr = Manager::from_config(config, tls, mgr_config);
    let pool = Pool::builder(mgr).build().unwrap();

    DatabasePool(pool)
}

pub fn connect_with_builder<T>(
    config: Config,
    tls: T,
    build: impl Fn(PoolBuilder) -> PoolBuilder,
) -> DatabasePool 
    where
        T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,

{
    let mgr_config = ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
        tag: None,
    };
    let mgr = Manager::from_config(config, tls, mgr_config);
    let builder = build(Pool::builder(mgr));
    builder.build().map(DatabasePool).unwrap()
}

pub fn connect_multi_with_builder<T>(
    config: Config,
    tls: T,
    build: BTreeMap<String, Box<dyn Fn(PoolBuilder) -> PoolBuilder>>,
) -> BTreeMap<Arc<String>, DatabasePool>
    where
        T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket> + Clone + Sync + Send + 'static,
        T::Stream: Sync + Send,
        T::TlsConnect: Sync + Send,
        <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,

    {
    build
        .into_iter()
        .map(|(key, build)| {
            let mgr_config = ManagerConfig {
                recycling_method: RecyclingMethod::Fast,
                tag: Some(key),
            };

            let mgr = Manager::from_config(config.clone(), tls.clone(), mgr_config);
            let builder = build(Pool::builder(mgr));
            let pool = builder.build().map(DatabasePool).unwrap();

            (pool.tag(), pool)
        })
        .collect()
}
