use crate::{query_builder::ManyRelatedRecordsWithRowNumber, FromSource, SqlCapabilities, Transaction, Transactional};
use tokio_executor::threadpool::blocking;
use futures::future::{FutureExt, ok, err, BoxFuture, poll_fn};
use datamodel::Source;
use prisma_query::{
    connector::{self, SqliteParams, Queryable},
    pool::{sqlite::SqliteConnectionManager, PrismaConnectionManager},
};
use std::convert::TryFrom;

type Pool = r2d2::Pool<PrismaConnectionManager<SqliteConnectionManager>>;

pub struct Sqlite {
    pool: Pool,
    file_path: String,
}

impl Sqlite {
    pub fn new(file_path: String, connection_limit: u32) -> crate::Result<Self> {
        let manager = PrismaConnectionManager::sqlite(None, &file_path)?;
        let pool = r2d2::Pool::builder().max_size(connection_limit).build(manager)?;

        Ok(Self {
            pool,
            file_path,
        })
    }

    pub fn file_path(&self) -> &str {
        self.file_path.as_str()
    }
}

impl FromSource for Sqlite {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let params = SqliteParams::try_from(source.url().value.as_str())?;
        let file_path = params.file_path.clone();
        let pool = r2d2::Pool::try_from(params).unwrap();

        Ok(Sqlite {
            pool,
            file_path: file_path.to_str().unwrap().to_string(),
        })
    }
}

impl SqlCapabilities for Sqlite {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl Transaction for connector::Sqlite {}

impl Transactional for Sqlite {
    fn with_transaction<F, T>(&self, db: &str, f: F) -> BoxFuture<'static, crate::Result<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&mut dyn Transaction) -> crate::Result<T> + Send + Sync + 'static,
    {
        let pool = self.pool.clone();
        let db = db.to_string();
        let mut f_cell = Some(f);

        let fut = poll_fn(move |_| {
            blocking(|| {
                let f = f_cell.take().unwrap();
                let mut conn = pool.get()?;

                conn.attach_database(db.as_str())?;
                conn.execute_raw("PRAGMA foreign_keys = ON", &[])?;

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

    fn with_connection<F, T>(&self, db: &str, f: F) -> BoxFuture<'static, crate::Result<T>>
    where
        T: Send + Sync + 'static,
        F: FnOnce(&mut dyn Transaction) -> crate::Result<T> + Send + Sync + 'static,
    {
        let pool = self.pool.clone();
        let db = db.to_string();
        let mut f_cell = Some(f);

        let fut = poll_fn(move |_| {
            blocking(|| {
                let f = f_cell.take().unwrap();
                let mut conn = pool.get()?;

                conn.attach_database(db.as_str())?;
                conn.execute_raw("PRAGMA foreign_keys = ON", &[])?;

                let result = f(&mut *conn);

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
}
