mod mysql;
mod postgresql;
mod sqlite;

use crate::{query_builder::*, Transactional};
use datamodel::Source;

pub use mysql::*;
pub use postgresql::*;
pub use sqlite::*;

pub trait FromSource {
    fn from_source(source: &dyn Source) -> crate::Result<Self>
    where
        Self: Transactional + SqlCapabilities + Sized;
}

pub trait SqlCapabilities {
    /// This we use to differentiate between databases with or without
    /// `ROW_NUMBER` function for related records pagination.
    type ManyRelatedRecordsBuilder: ManyRelatedRecordsQueryBuilder;
}

/// A wrapper for relational databases due to trait restrictions. Implements the
/// needed traits.
pub struct SqlDatabase<T>
where
    T: Transactional + SqlCapabilities + Send + Sync + 'static,
{
    pub executor: T,
}

impl<T> SqlDatabase<T>
where
    T: Transactional + SqlCapabilities + Send + Sync + 'static,
{
    pub fn new(executor: T) -> Self {
        Self { executor }
    }
}
