//! Backend ownership in the existing durable store, not another task scheduler.
//! Non-cleaned records quarantine uncertainty; they are not execution receipts.

use std::collections::BTreeMap;

use hub_core::{AttemptId, CoreError, LifecycleEntityType, LifecycleEventKind, NewLifecycleEvent};
use rusqlite::OptionalExtension as _;
use serde::{Deserialize, Serialize};

use crate::{append_event_tx, storage, storage_now_ms, SqliteStore};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SandboxAttemptState {
    Reserved,
    Owned,
    Cleaned,
}

impl SandboxAttemptState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Reserved => "reserved",
            Self::Owned => "owned",
            Self::Cleaned => "cleaned",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SandboxAttempt {
    pub task_id: String,
    pub attempt_id: AttemptId,
    pub backend: String,
    pub request_sha256: String,
    pub workspace: String,
    pub state: SandboxAttemptState,
    pub owned_handle: Option<String>,
}

impl SandboxAttempt {
    fn validate(&self) -> Result<(), CoreError> {
        let token = |s: &str, cap| {
            !s.is_empty()
                && s.len() <= cap
                && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-')
        };
        if !token(&self.task_id, 256)
            || !token(&self.backend, 64)
            || self.request_sha256.len() != 64
            || !self
                .request_sha256
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            || self.workspace.len() > 4096
            || !self.workspace.starts_with('/')
            || self.workspace.chars().any(char::is_control)
            || (self.state == SandboxAttemptState::Reserved) != self.owned_handle.is_none()
            || self.owned_handle.as_ref().is_some_and(|s| !token(s, 256))
        {
            return Err(CoreError::Validation(
                "invalid sandbox attempt ownership record".into(),
            ));
        }
        Ok(())
    }
}

impl SqliteStore {
    /// Reserve once before side effects. Even cleaned task IDs cannot be replayed.
    pub fn reserve_sandbox_attempt(
        &self,
        task_id: &str,
        backend: &str,
        request_sha256: &str,
        workspace: &str,
    ) -> Result<SandboxAttempt, CoreError> {
        let record = SandboxAttempt {
            task_id: task_id.into(),
            attempt_id: AttemptId::generate(),
            backend: backend.into(),
            request_sha256: request_sha256.into(),
            workspace: workspace.into(),
            state: SandboxAttemptState::Reserved,
            owned_handle: None,
        };
        record.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning sandbox reservation"))?;
        let inserted = tx
            .execute(
                "INSERT INTO sandbox_attempts (task_id, workspace, state, record_json)
             SELECT ?1, ?2, 'reserved', ?3
             WHERE (SELECT COUNT(*) FROM sandbox_attempts WHERE state != 'cleaned') < 256
             AND NOT EXISTS (SELECT 1 FROM sandbox_attempts WHERE state != 'cleaned' AND
                (workspace = ?2 OR substr(?2, 1, length(workspace) + 1) = workspace || '/'
                 OR substr(workspace, 1, length(?2) + 1) = ?2 || '/'))",
                rusqlite::params![
                    task_id,
                    workspace,
                    serde_json::to_string(&record).map_err(json_error)?
                ],
            )
            .map_err(|_| {
                CoreError::Storage(
                    "task ID already journaled or reservation unavailable; never replay it".into(),
                )
            })?;
        if inserted != 1 {
            return Err(CoreError::Validation(
                "workspace overlap quarantined or uncleaned attempt capacity exhausted".into(),
            ));
        }
        sandbox_event(&tx, &record)?;
        tx.commit()
            .map_err(storage("committing sandbox reservation"))?;
        Ok(record)
    }

    pub fn sandbox_attempt(&self, task_id: &str) -> Result<Option<SandboxAttempt>, CoreError> {
        let conn = self.lock()?;
        let raw: Option<String> = conn
            .query_row(
                "SELECT record_json FROM sandbox_attempts WHERE task_id = ?1",
                [task_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage("reading sandbox attempt"))?;
        raw.map(|s| {
            let record: SandboxAttempt = serde_json::from_str(&s).map_err(json_error)?;
            record.validate()?;
            Ok(record)
        })
        .transpose()
    }

    /// Trusted backend observation port, not standalone ownership/isolation proof.
    pub fn sandbox_own(
        &self,
        task_id: &str,
        attempt_id: AttemptId,
        handle: &str,
    ) -> Result<SandboxAttempt, CoreError> {
        self.sandbox_transition(
            task_id,
            attempt_id,
            handle,
            SandboxAttemptState::Reserved,
            SandboxAttemptState::Owned,
        )
    }

    /// Invoke only after a successful exact-handle absence observation.
    pub fn sandbox_confirm_absent(
        &self,
        task_id: &str,
        attempt_id: AttemptId,
        handle: &str,
    ) -> Result<SandboxAttempt, CoreError> {
        self.sandbox_transition(
            task_id,
            attempt_id,
            handle,
            SandboxAttemptState::Owned,
            SandboxAttemptState::Cleaned,
        )
    }

    fn sandbox_transition(
        &self,
        task_id: &str,
        attempt_id: AttemptId,
        handle: &str,
        expected: SandboxAttemptState,
        next: SandboxAttemptState,
    ) -> Result<SandboxAttempt, CoreError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning sandbox transition"))?;
        let raw: String = tx
            .query_row(
                "SELECT record_json FROM sandbox_attempts WHERE task_id = ?1",
                [task_id],
                |r| r.get(0),
            )
            .map_err(storage("loading sandbox ownership"))?;
        let mut record: SandboxAttempt = serde_json::from_str(&raw).map_err(json_error)?;
        record.validate()?;
        if record.state != expected
            || record.attempt_id != attempt_id
            || record.owned_handle.as_ref().is_some_and(|h| h != handle)
        {
            return Err(CoreError::Validation(
                "stale sandbox ownership transition".into(),
            ));
        }
        record.state = next;
        record.owned_handle = Some(handle.into());
        record.validate()?;
        tx.execute(
            "UPDATE sandbox_attempts SET record_json = ?2, state = ?3 WHERE task_id = ?1",
            rusqlite::params![
                task_id,
                serde_json::to_string(&record).map_err(json_error)?,
                next.as_str()
            ],
        )
        .map_err(storage("updating sandbox ownership"))?;
        sandbox_event(&tx, &record)?;
        tx.commit()
            .map_err(storage("committing sandbox transition"))?;
        Ok(record)
    }
}

fn sandbox_event(tx: &rusqlite::Transaction<'_>, record: &SandboxAttempt) -> Result<(), CoreError> {
    append_event_tx(
        tx,
        &NewLifecycleEvent::new(
            storage_now_ms(),
            LifecycleEventKind::SandboxAttemptStateChanged,
            LifecycleEntityType::SandboxAttempt,
            record.attempt_id.to_string(),
            BTreeMap::from([
                ("task_id".into(), record.task_id.clone()),
                ("state".into(), record.state.as_str().into()),
                ("request_sha256".into(), record.request_sha256.clone()),
            ]),
        ),
    )
    .map(|_| ())
}

fn json_error(error: serde_json::Error) -> CoreError {
    CoreError::Storage(format!("sandbox ownership JSON: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_core::LifecycleEventRepository;

    #[test]
    fn restart_preserves_uncertainty_ownership_and_refuses_every_replay() {
        let root = std::env::temp_dir().join(format!("hub-attempt-{}", AttemptId::generate()));
        let db = root.join("hub.sqlite");
        let reserved = SqliteStore::open(&db)
            .unwrap()
            .reserve_sandbox_attempt("task-1", "docker-runsc", &"a".repeat(64), "/owned/task")
            .unwrap();
        let store = SqliteStore::open(&db).unwrap();
        assert_eq!(store.sandbox_attempt("task-1").unwrap().unwrap(), reserved);
        assert!(store
            .reserve_sandbox_attempt("task-1", "docker-runsc", &"b".repeat(64), "/owned/task")
            .is_err());
        assert!(store
            .reserve_sandbox_attempt("task-2", "docker-runsc", &"b".repeat(64), "/owned/task")
            .is_err());
        assert!(store
            .reserve_sandbox_attempt(
                "task-2",
                "docker-runsc",
                &"b".repeat(64),
                "/owned/task/child"
            )
            .is_err());
        assert!(store
            .reserve_sandbox_attempt("task-2", "docker-runsc", &"b".repeat(64), "/owned")
            .is_err());
        assert!(store
            .sandbox_confirm_absent("task-1", reserved.attempt_id, &"c".repeat(64))
            .is_err());
        assert!(store
            .sandbox_own("task-1", AttemptId::generate(), &"c".repeat(64))
            .is_err());
        let owned = store
            .sandbox_own("task-1", reserved.attempt_id, &"c".repeat(64))
            .unwrap();
        assert!(store
            .sandbox_confirm_absent("task-1", owned.attempt_id, &"d".repeat(64))
            .is_err());
        drop(store);
        let store = SqliteStore::open(&db).unwrap();
        assert_eq!(store.sandbox_attempt("task-1").unwrap().unwrap(), owned);
        let cleaned = store
            .sandbox_confirm_absent("task-1", owned.attempt_id, &"c".repeat(64))
            .unwrap();
        assert_eq!(cleaned.state, SandboxAttemptState::Cleaned);
        assert!(store
            .reserve_sandbox_attempt("task-1", "docker-runsc", &"a".repeat(64), "/owned/task")
            .is_err());
        assert_eq!(store.list_after(0, 10).unwrap().len(), 3);
        assert!(store
            .reserve_sandbox_attempt("task-2", "docker-runsc", &"a".repeat(64), "/owned/task")
            .is_ok());
        drop(store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_connections_reserve_a_single_authoritative_attempt() {
        let root = std::env::temp_dir().join(format!("hub-attempt-{}", AttemptId::generate()));
        let db = root.join("hub.sqlite");
        let first = SqliteStore::open(&db).unwrap();
        let second = SqliteStore::open(&db).unwrap();
        let a = std::thread::spawn(move || {
            first.reserve_sandbox_attempt("task-1", "docker-runsc", &"a".repeat(64), "/owned/task")
        });
        let b = second.reserve_sandbox_attempt(
            "task-1",
            "docker-runsc",
            &"b".repeat(64),
            "/owned/task",
        );
        let a = a.join().unwrap();
        assert_ne!(a.is_ok(), b.is_ok());
        drop(second);
        std::fs::remove_dir_all(root).unwrap();
    }
}
