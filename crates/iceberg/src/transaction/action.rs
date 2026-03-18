// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::mem::take;
use std::sync::Arc;

use as_any::AsAny;
use async_trait::async_trait;

use crate::table::Table;
use crate::transaction::Transaction;
use crate::{Result, TableRequirement, TableUpdate};

/// A boxed, thread-safe reference to a `TransactionAction`.
pub(crate) type BoxedTransactionAction = Arc<dyn TransactionAction>;

/// A trait representing an atomic action that can be part of a transaction.
///
/// Implementors of this trait define how a specific action is committed to a table.
/// Each action is responsible for generating the updates and requirements needed
/// to modify the table metadata.
///
/// # Overview
///
/// `TransactionAction` is the core abstraction for operations that modify Iceberg tables.
/// Actions are applied to a [`Transaction`] using the [`ApplyTransactionAction`] trait,
/// and when the transaction is committed, each action's `commit` method is called in sequence
/// to generate the necessary table updates and requirements.
///
/// # Built-in Actions
///
/// The library provides several built-in actions:
/// - `FastAppendAction` - Appends data files without rewriting existing manifests
/// - `UpdateLocationAction` - Updates the table's metadata location
/// - `UpdatePropertiesAction` - Updates table properties
/// - `UpdateStatisticsAction` - Updates table statistics
/// - `UpgradeFormatVersionAction` - Upgrades the table format version
/// - `ReplaceSortOrderAction` - Replaces the table's sort order
///
/// # Custom Actions
///
/// You can implement this trait to create custom table operations. Your implementation
/// should generate appropriate [`TableUpdate`]s and [`TableRequirement`]s based on the
/// desired changes.
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use async_trait::async_trait;
/// use iceberg::transaction::action::{ActionCommit, TransactionAction};
/// use iceberg::table::Table;
/// use iceberg::{Result, TableUpdate};
///
/// struct MyCustomAction {
///     // action-specific fields
/// }
///
/// #[async_trait]
/// impl TransactionAction for MyCustomAction {
///     async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit> {
///         // Generate updates based on the action
///         let updates = vec![
///             TableUpdate::SetProperties {
///                 updates: [("my.property".to_string(), "value".to_string())]
///                     .into_iter()
///                     .collect(),
///             }
///         ];
///
///         Ok(ActionCommit::new(updates, vec![]))
///     }
/// }
/// ```
#[async_trait]
pub trait TransactionAction: AsAny + Sync + Send {
    /// Commits this action against the provided table and returns the resulting updates.
    ///
    /// This method is called by the transaction framework when the transaction is committed.
    /// It should generate the appropriate [`TableUpdate`]s and [`TableRequirement`]s for
    /// this action.
    ///
    /// # Arguments
    ///
    /// * `table` - The current state of the table this action should apply to.
    ///
    /// # Returns
    ///
    /// An [`ActionCommit`] containing table updates and table requirements,
    /// or an error if the commit fails.
    ///
    /// # Note
    ///
    /// This method is typically called by the transaction framework and should not be
    /// called directly by users. Instead, use [`ApplyTransactionAction::apply`] to add
    /// actions to a transaction, then call [`Transaction::commit`] to execute all actions.
    async fn commit(self: Arc<Self>, table: &Table) -> Result<ActionCommit>;
}

/// A helper trait for applying a `TransactionAction` to a `Transaction`.
///
/// This is implemented for all `TransactionAction` types
/// to allow easy chaining of actions into a transaction context.
pub trait ApplyTransactionAction {
    /// Adds this action to the given transaction.
    ///
    /// # Arguments
    ///
    /// * `tx` - The transaction to apply the action to.
    ///
    /// # Returns
    ///
    /// The modified transaction containing this action, or an error if the operation fails.
    fn apply(self, tx: Transaction) -> Result<Transaction>;
}

impl<T: TransactionAction + 'static> ApplyTransactionAction for T {
    fn apply(self, mut tx: Transaction) -> Result<Transaction>
    where Self: Sized {
        tx.actions.push(Arc::new(self));
        Ok(tx)
    }
}

/// The result of committing a `TransactionAction`.
///
/// This struct contains the updates to apply to the table's metadata
/// and any preconditions that must be satisfied before the update can be committed.
pub struct ActionCommit {
    updates: Vec<TableUpdate>,
    requirements: Vec<TableRequirement>,
}

impl ActionCommit {
    /// Creates a new `ActionCommit` from the given updates and requirements.
    pub fn new(updates: Vec<TableUpdate>, requirements: Vec<TableRequirement>) -> Self {
        Self {
            updates,
            requirements,
        }
    }

    /// Consumes and returns the list of table updates.
    pub fn take_updates(&mut self) -> Vec<TableUpdate> {
        take(&mut self.updates)
    }

    /// Consumes and returns the list of table requirements.
    pub fn take_requirements(&mut self) -> Vec<TableRequirement> {
        take(&mut self.requirements)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;

    use as_any::Downcast;
    use async_trait::async_trait;
    use uuid::Uuid;

    use crate::table::Table;
    use crate::transaction::Transaction;
    use crate::transaction::action::{ActionCommit, ApplyTransactionAction, TransactionAction};
    use crate::transaction::tests::make_v2_table;
    use crate::{Result, TableRequirement, TableUpdate};

    struct TestAction;

    #[async_trait]
    impl TransactionAction for TestAction {
        async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
            Ok(ActionCommit::new(
                vec![TableUpdate::SetLocation {
                    location: String::from("s3://bucket/prefix/table/"),
                }],
                vec![TableRequirement::UuidMatch {
                    uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1")?,
                }],
            ))
        }
    }

    #[tokio::test]
    async fn test_commit_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;

        let mut action_commit = Arc::new(action).commit(&table).await.unwrap();

        let updates = action_commit.take_updates();
        let requirements = action_commit.take_requirements();

        assert_eq!(updates[0], TableUpdate::SetLocation {
            location: String::from("s3://bucket/prefix/table/")
        });
        assert_eq!(requirements[0], TableRequirement::UuidMatch {
            uuid: Uuid::from_str("9c12d441-03fe-4693-9a96-a0705ddf69c1").unwrap()
        });
    }

    #[test]
    fn test_apply_transaction_action() {
        let table = make_v2_table();
        let action = TestAction;
        let tx = Transaction::new(&table);

        let updated_tx = action.apply(tx).unwrap();
        // There should be one action in the transaction now
        assert_eq!(updated_tx.actions.len(), 1);

        (*updated_tx.actions[0])
            .downcast_ref::<TestAction>()
            .expect("TestAction was not applied to Transaction!");
    }

    #[test]
    fn test_action_commit() {
        // Create dummy updates and requirements
        let location = String::from("s3://bucket/prefix/table/");
        let uuid = Uuid::new_v4();
        let updates = vec![TableUpdate::SetLocation { location }];
        let requirements = vec![TableRequirement::UuidMatch { uuid }];

        let mut action_commit = ActionCommit::new(updates.clone(), requirements.clone());

        let taken_updates = action_commit.take_updates();
        let taken_requirements = action_commit.take_requirements();

        // Check values are returned correctly
        assert_eq!(taken_updates, updates);
        assert_eq!(taken_requirements, requirements);

        assert!(action_commit.take_updates().is_empty());
        assert!(action_commit.take_requirements().is_empty());
    }
}
