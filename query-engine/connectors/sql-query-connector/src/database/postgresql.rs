use crate::{query_builder::ManyRelatedRecordsWithRowNumber, FromSource, SqlCapabilities, Transaction, Transactional};
use tokio_executor::threadpool::blocking;
use futures::future::{FutureExt, ok, err, BoxFuture, poll_fn};
use datamodel::Source;
use prisma_query::{
    connector::{self, PostgresParams, Queryable},
    pool::{postgres::PostgresManager, PrismaConnectionManager},
};
use std::convert::TryFrom;

type Pool = r2d2::Pool<PrismaConnectionManager<PostgresManager>>;

pub struct PostgreSql {
    pool: Pool,
}

impl FromSource for PostgreSql {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let url = url::Url::parse(&source.url().value)?;
        let params = PostgresParams::try_from(url)?;
        let pool = r2d2::Pool::try_from(params).unwrap();

        Ok(PostgreSql { pool })
    }
}

impl SqlCapabilities for PostgreSql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl Transaction for connector::PostgreSql {}

impl Transactional for PostgreSql {
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
