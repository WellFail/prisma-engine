use crate::commands::command::*;
use crate::migration_engine::MigrationEngine;
use migration_connector::*;
use serde_json::json;

pub struct ResetCommand;

#[async_trait::async_trait]
impl MigrationCommand for ResetCommand {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    fn new(_: Self::Input) -> Box<Self> {
        Box::new(Self)
    }

    async fn execute<C, D>(&self, engine: &MigrationEngine<C, D>) -> CommandResult<Self::Output>
    where
        C: MigrationConnector<DatabaseMigration = D>,
        D: DatabaseMigrationMarker + 'static,
    {
        engine.reset()?;
        engine.init()?;

        Ok(json!({}))
    }
}
