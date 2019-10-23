use crate::{query_builder::ManyRelatedRecordsWithUnionAll, FromSource, SqlCapabilities, Transaction, Transactional};
use datamodel::Source;
use prisma_query::pool::{self, MysqlManager};
use tokio_resource_pool::{CheckOut, Pool};
use url::Url;

pub struct Mysql {
    pool: Pool<MysqlManager>,
}

impl FromSource for Mysql {
    fn from_source(source: &dyn Source) -> crate::Result<Self> {
        let url = Url::parse(&source.url().value)?;
        let pool = pool::mysql(url)?;

        Ok(Mysql { pool })
    }
}

impl SqlCapabilities for Mysql {
    type ManyRelatedRecordsBuilder = ManyRelatedRecordsWithUnionAll;
}

impl Transaction for CheckOut<MysqlManager> {}

impl Transactional for Mysql {
    fn get_connection<'a>(&'a self, _: &'a str) -> crate::IO<'a, Box<dyn Transaction>> {
        crate::IO::new(async move {
            let conn = self.pool.check_out().await?;
            Ok(Box::new(conn) as Box<dyn Transaction>)
        })
    }
}
