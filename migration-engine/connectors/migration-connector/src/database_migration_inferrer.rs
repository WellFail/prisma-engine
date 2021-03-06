use crate::{ConnectorResult, MigrationStep};
use datamodel::Datamodel;

/// The component responsible for generating a [DatabaseMigration](trait.MigrationConnector.html#associatedtype.DatabaseMigration)
/// migrating the database from one datamodel to another. In addition to the datamodel information provided by the core, a connector
/// may gather additional information itself, e.g. through introspection of the underlying database.
pub trait DatabaseMigrationInferrer<T>: Send + Sync + 'static {
    /// Infer the database migration steps. The previous datamodel is provided, but the implementor can ignore it.
    fn infer(&self, previous: &Datamodel, next: &Datamodel, steps: &[MigrationStep]) -> ConnectorResult<T>;

    /// Infer a database migration based on the previous and next datamodels. The method signature is identical to `infer`,
    /// but it is expected that this method is implemented based on the provided previous datamodel, and does not rely
    /// on the current state of the database.
    fn infer_from_datamodels(
        &self,
        previous: &Datamodel,
        next: &Datamodel,
        steps: &[MigrationStep],
    ) -> ConnectorResult<T>;
}
