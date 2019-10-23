use crate::{query_builder::ManyRelatedRecordsWithRowNumber, FromSource, SqlCapabilities, Transaction, Transactional};
use datamodel::Source;
use prisma_query::{
    connector::{Queryable, SqliteParams},
    pool::{self, SqliteManager},
};
use std::convert::TryFrom;
use tokio_resource_pool::{CheckOut, Pool};

pub struct Sqlite {
    pool: Pool<SqliteManager>,
    file_path: String,
}

impl Sqlite {
    pub fn new(file_path: String) -> crate::Result<Self> {
        let pool = pool::sqlite(&file_path)?;

        Ok(Self { pool, file_path })
    }

    pub fn file_path(&self) -> &str {
        self.file_path.as_str()
    }
}

impl FromSource for Sqlite {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let params = SqliteParams::try_from(source.url().value.as_str())?;
        let file_path = params.file_path.to_str().unwrap().to_string();

        Self::new(file_path)
    }
}

impl SqlCapabilities for Sqlite {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl Transaction for CheckOut<SqliteManager> {}

impl Transactional for Sqlite {
    fn get_connection<'a>(&'a self, db: &'a str) -> crate::IO<'a, Box<dyn Transaction>> {
        crate::IO::new(async move {
            let mut conn = self.pool.check_out().await?;

            conn.attach_database(db)?;
            conn.execute_raw("PRAGMA foreign_keys = ON", &[]).await?;

            Ok(Box::new(conn) as Box<dyn Transaction>)
        })
    }
}
