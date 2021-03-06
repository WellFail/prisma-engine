use crate::test_harness::{IntrospectionDatabase, IntrospectionDatabaseWrapper, Mysql, PostgreSql, Sqlite};
use barrel::{Migration, SqlVariant};
use introspection_connector::IntrospectionConnector;
use pretty_assertions::assert_eq;
use prisma_query::connector::{MysqlParams, PostgresParams};
use sql_introspection_connector::*;
use std::{convert::TryFrom, rc::Rc, sync::Arc};
use url::Url;

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum SqlFamily {
    Sqlite,
    Postgres,
    Mysql,
}

pub struct TestSetup {
    pub sql_family: SqlFamily,
    pub database: Arc<dyn IntrospectionDatabase + Send + Sync + 'static>,
    pub introspection_connector: Box<dyn IntrospectionConnector>,
}

pub struct BarrelMigrationExecutor {
    database: Arc<dyn IntrospectionDatabase + Send + Sync>,
    sql_variant: barrel::backend::SqlVariant,
}

// test execution

pub(crate) fn custom_assert(left: &str, right: &str) {
    let parsed_expected = datamodel::parse_datamodel(&right).unwrap();
    let reformatted_expected =
        datamodel::render_datamodel_to_string(&parsed_expected).expect("Datamodel rendering failed");

    assert_eq!(left, reformatted_expected);
}

pub(crate) fn introspect(test_setup: &TestSetup) -> String {
    let datamodel = test_setup.introspection_connector.introspect(SCHEMA_NAME).unwrap();
    datamodel::render_datamodel_to_string(&datamodel).expect("Datamodel rendering failed")
}

fn run_full_sql(database: &Arc<dyn IntrospectionDatabase + Send + Sync>, full_sql: &str) {
    for sql in full_sql.split(";") {
        if sql != "" {
            database.query_raw(SCHEMA_NAME, &sql, &[]).unwrap();
        }
    }
}

pub(crate) fn test_each_backend<F>(test_fn: F)
where
    F: Fn(&TestSetup, &BarrelMigrationExecutor) -> () + std::panic::RefUnwindSafe,
{
    test_each_backend_with_ignores(Vec::new(), test_fn);
}

pub(crate) fn test_each_backend_with_ignores<F>(ignores: Vec<SqlFamily>, test_fn: F)
where
    F: Fn(&TestSetup, &BarrelMigrationExecutor) -> () + std::panic::RefUnwindSafe,
{
    //     SQLite
    if !ignores.contains(&SqlFamily::Sqlite) {
        println!("Testing with SQLite now");
        let test_setup = get_sqlite();

        println!("Running the test function now");
        let barrel_migration_executor = BarrelMigrationExecutor {
            database: Arc::clone(&test_setup.database),
            sql_variant: SqlVariant::Sqlite,
        };

        test_fn(&test_setup, &barrel_migration_executor);
    } else {
        println!("Ignoring SQLite")
    }
    // POSTGRES
    if !ignores.contains(&SqlFamily::Postgres) {
        println!("Testing with Postgres now");
        let test_setup = get_postgres();

        println!("Running the test function now");
        let barrel_migration_executor = BarrelMigrationExecutor {
            database: Arc::clone(&test_setup.database),
            sql_variant: SqlVariant::Pg,
        };

        test_fn(&test_setup, &barrel_migration_executor);
    } else {
        println!("Ignoring Postgres")
    }
    // MySQL
    if !ignores.contains(&SqlFamily::Mysql) {
        println!("Testing with MySql now");
        let test_setup = get_mysql();
        println!("Running the test function now");
        let barrel_migration_executor = BarrelMigrationExecutor {
            database: Arc::clone(&test_setup.database),
            sql_variant: SqlVariant::Mysql,
        };

        test_fn(&test_setup, &barrel_migration_executor);
    } else {
        println!("Ignoring MySql")
    }
}

// barrel

impl BarrelMigrationExecutor {
    pub fn execute<F>(&self, mut migration_fn: F)
    where
        F: FnMut(&mut Migration) -> (),
    {
        let mut migration = Migration::new().schema(SCHEMA_NAME);
        migration_fn(&mut migration);
        let full_sql = dbg!(migration.make_from(self.sql_variant));
        run_full_sql(&self.database, &full_sql);
    }
}

// get dbs

pub fn database(sql_family: SqlFamily, database_url: &str) -> Box<dyn IntrospectionDatabase + Send + Sync + 'static> {
    match sql_family {
        SqlFamily::Postgres => {
            let url = Url::parse(database_url).unwrap();
            let create_cmd = |name| format!("CREATE DATABASE \"{}\"", name);

            let connect_cmd = |url| {
                let params = PostgresParams::try_from(url)?;
                PostgreSql::new(params, true)
            };

            let conn = with_database(url, "postgres", "postgres", create_cmd, Rc::new(connect_cmd));

            Box::new(conn)
        }
        SqlFamily::Sqlite => Box::new(Sqlite::new(database_url).unwrap()),
        SqlFamily::Mysql => {
            let url = Url::parse(database_url).unwrap();
            let create_cmd = |name| format!("CREATE DATABASE `{}`", name);

            let connect_cmd = |url| {
                let params = MysqlParams::try_from(url)?;
                Mysql::new(params, true)
            };

            let conn = with_database(url, "mysql", "/", create_cmd, Rc::new(connect_cmd));

            Box::new(conn)
        }
    }
}

pub fn database_wrapper(sql_family: SqlFamily, database_url: &str) -> IntrospectionDatabaseWrapper {
    IntrospectionDatabaseWrapper {
        database: database(sql_family, database_url).into(),
    }
}

fn with_database<F, T, S>(url: Url, default_name: &str, root_path: &str, create_stmt: S, f: Rc<F>) -> T
where
    T: IntrospectionDatabase,
    F: Fn(Url) -> Result<T, prisma_query::error::Error>,
    S: FnOnce(String) -> String,
{
    match f(url.clone()) {
        Ok(conn) => conn,
        Err(_) => {
            create_database(url.clone(), default_name, root_path, create_stmt, f.clone());
            f(url).unwrap()
        }
    }
}

fn create_database<F, T, S>(url: Url, default_name: &str, root_path: &str, create_stmt: S, f: Rc<F>)
where
    T: IntrospectionDatabase,
    F: Fn(Url) -> Result<T, prisma_query::error::Error>,
    S: FnOnce(String) -> String,
{
    let db_name = fetch_db_name(&url, default_name);

    let mut url = url.clone();
    url.set_path(root_path);

    let conn = f(url).unwrap();

    conn.execute_raw("", &create_stmt(db_name), &[]).unwrap();
}

fn fetch_db_name(url: &Url, default: &str) -> String {
    let result = match url.path_segments() {
        Some(mut segments) => segments.next().unwrap_or(default),
        None => default,
    };

    String::from(result)
}

fn get_sqlite() -> TestSetup {
    let wrapper = database_wrapper(SqlFamily::Sqlite, &sqlite_test_file());
    let database = Arc::clone(&wrapper.database);

    let database_file_path = sqlite_test_file();
    let _ = std::fs::remove_file(database_file_path.clone()); // ignore potential errors
    let introspection_connector = SqlIntrospectionConnector::new(&sqlite_test_url()).unwrap();

    TestSetup {
        database,
        sql_family: SqlFamily::Sqlite,
        introspection_connector: Box::new(introspection_connector),
    }
}

fn get_postgres() -> TestSetup {
    let wrapper = database_wrapper(SqlFamily::Postgres, &postgres_url());
    let database = Arc::clone(&wrapper.database);

    let drop_schema = dbg!(format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE;", SCHEMA_NAME));
    let _ = database.query_raw(SCHEMA_NAME, &drop_schema, &[]);

    let create_schema = dbg!(format!("CREATE SCHEMA IF NOT EXISTS \"{}\";", SCHEMA_NAME));
    let _ = database.query_raw(SCHEMA_NAME, &create_schema, &[]);

    let introspection_connector = SqlIntrospectionConnector::new(&postgres_url()).unwrap();

    TestSetup {
        database,
        sql_family: SqlFamily::Postgres,
        introspection_connector: Box::new(introspection_connector),
    }
}

fn get_mysql() -> TestSetup {
    let wrapper = database_wrapper(SqlFamily::Mysql, &mysql_url());
    let database = Arc::clone(&wrapper.database);

    let drop_schema = dbg!(format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE;", SCHEMA_NAME));
    let _ = database.query_raw(SCHEMA_NAME, &drop_schema, &[]);

    let introspection_connector = SqlIntrospectionConnector::new(&mysql_url()).unwrap();

    TestSetup {
        database,
        sql_family: SqlFamily::Mysql,
        introspection_connector: Box::new(introspection_connector),
    }
}

// urls
pub const SCHEMA_NAME: &str = "introspection-engine";

pub fn sqlite_test_url() -> String {
    format!("file:{}", sqlite_test_file())
}

pub fn sqlite_test_file() -> String {
    let server_root = std::env::var("SERVER_ROOT").expect("Env var SERVER_ROOT required but not found.");
    let database_folder_path = format!("{}/db", server_root);
    let file_path = format!("{}/{}.db", database_folder_path, SCHEMA_NAME);
    file_path
}

pub fn postgres_url() -> String {
    dbg!(format!(
        "postgresql://postgres:prisma@{}:5432/test-db?schema={}",
        db_host_postgres(),
        SCHEMA_NAME
    ))
}

pub fn mysql_url() -> String {
    dbg!(format!(
        "mysql://root:prisma@{}:3306/{}",
        db_host_mysql_5_7(),
        SCHEMA_NAME
    ))
}

pub fn mysql_8_url() -> String {
    let (host, port) = db_host_and_port_mysql_8_0();
    dbg!(format!(
        "mysql://root:prisma@{host}:{port}/{schema_name}",
        host = host,
        port = port,
        schema_name = SCHEMA_NAME
    ))
}

fn db_host_postgres() -> String {
    match std::env::var("IS_BUILDKITE") {
        Ok(_) => "test-db-postgres".to_string(),
        Err(_) => "127.0.0.1".to_string(),
    }
}

fn db_host_and_port_mysql_8_0() -> (String, usize) {
    match std::env::var("IS_BUILDKITE") {
        Ok(_) => ("test-db-mysql-8-0".to_string(), 3306),
        Err(_) => ("127.0.0.1".to_string(), 3307),
    }
}

fn db_host_mysql_5_7() -> String {
    match std::env::var("IS_BUILDKITE") {
        Ok(_) => "test-db-mysql-5-7".to_string(),
        Err(_) => "127.0.0.1".to_string(),
    }
}
