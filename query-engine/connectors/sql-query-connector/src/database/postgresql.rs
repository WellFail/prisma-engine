use crate::{query_builder::ManyRelatedRecordsWithRowNumber, FromSource, SqlCapabilities, Transaction, Transactional, SQLIO};
use datamodel::Source;
use prisma_query::{
    pool::{self, PostgresManager},
};
use tokio_resource_pool::{CheckOut, Pool};

pub struct PostgreSql {
    pool: Pool<PostgresManager>,
}

impl FromSource for PostgreSql {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let url = url::Url::parse(&source.url().value)?;
        let pool = pool::postgres(url)?;

        Ok(PostgreSql { pool })
    }
}

impl SqlCapabilities for PostgreSql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithRowNumber;
}

impl Transaction for CheckOut<PostgresManager> {}

impl Transactional for PostgreSql {
    fn get_connection<'a>(&'a self, _: &'a str) -> SQLIO<'a, Box<dyn Transaction>> {
        SQLIO::new(async move {
            let conn = self.pool.check_out().await?;
            Ok(Box::new(conn) as Box<dyn Transaction>)
        })
    }
}
