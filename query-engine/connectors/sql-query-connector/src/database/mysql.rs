use crate::{query_builder::ManyRelatedRecordsWithUnionAll, FromSource, SqlCapabilities, Transaction, Transactional};
use tokio_executor::threadpool::blocking;
use futures::future::{FutureExt, ok, err, BoxFuture, poll_fn};
use datamodel::Source;
use prisma_query::{
    connector::{self, MysqlParams, Queryable},
    pool::{mysql::MysqlConnectionManager, PrismaConnectionManager},
};
use std::convert::TryFrom;
use url::Url;

type Pool = r2d2::Pool<PrismaConnectionManager<MysqlConnectionManager>>;

pub struct Mysql {
    pool: Pool,
}

impl FromSource for Mysql {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let url = Url::parse(&source.url().value)?;
        let params = MysqlParams::try_from(url)?;
        let pool = r2d2::Pool::try_from(params).unwrap();

        Ok(Mysql { pool })
    }
}

impl SqlCapabilities for Mysql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithUnionAll;
}


impl Transaction for connector::Mysql {}

impl Transactional for Mysql {
    fn with_transaction<F, T>(&self, _: &str, f: F) -> BoxFuture<'static, crate::Result<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&mut dyn Transaction) -> crate::Result<T> + Send + Sync + 'static,
    {
        let pool = self.pool.clone();
        let mut f_cell = Some(f);

        let fut = poll_fn(move |_| {
            let f = f_cell.take().unwrap();

            blocking(|| {
                let mut conn = pool.get()?;
                let mut tx = conn.start_transaction()?;
                let result = f(&mut tx);

                if result.is_ok() {
                    tx.commit()?;
                }

                result
            })
        }).then(|res| {
            match res {
                Ok(Ok(results)) => ok(results),
                Ok(Err(query_err)) => err(query_err),
                Err(blocking_err) => err(blocking_err.into())
            }
        });

        fut.boxed()
    }

    fn with_connection<F, T>(&self, _: &str, f: F) -> BoxFuture<'static, crate::Result<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&mut dyn Transaction) -> crate::Result<T> + Send + Sync + 'static,
    {
        let pool = self.pool.clone();
        let mut f_cell = Some(f);

        let fut = poll_fn(move |_| {
            let f = f_cell.take().unwrap();

            blocking(|| {
                let mut conn = pool.get()?;

                f(&mut *conn)
            })
        }).then(|res| {
            match res {
                Ok(Ok(results)) => ok(results),
                Ok(Err(query_err)) => err(query_err),
                Err(blocking_err) => err(blocking_err.into())
            }
        });

        fut.boxed()
    }
}
