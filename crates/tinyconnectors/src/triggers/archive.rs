//! Appending to and reading back the daily trigger archive stream.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tinystoragedrivers_api::StreamStore;
use tinystoragedrivers_types::Capability;

use crate::{ComposioTriggerHistoryEntry, ComposioTriggerHistoryResult, Error, Result};

/// Deliveries returned when a caller does not say how many it wants.
pub const DEFAULT_HISTORY_LIMIT: usize = 50;

/// Each day's stream is `STREAM_PREFIX` and the UTC date. The names are
/// declared once, in the contract crate, beside the migration that renames
/// them.
pub(crate) use tinyconnectors_bus::storage::{LEGACY_STREAM_PREFIX, STREAM_PREFIX};

/// A daily-rotated record of webhook deliveries, one stream per UTC day.
#[derive(Debug, Clone)]
pub struct TriggerArchive {
    streams: Arc<dyn StreamStore>,
}

impl TriggerArchive {
    /// Archive deliveries into `streams`.
    #[must_use]
    pub fn new(streams: Arc<dyn StreamStore>) -> Self {
        Self { streams }
    }

    /// The stream-name prefix the archive writes under.
    #[must_use]
    pub fn archive_dir(&self) -> &'static str {
        STREAM_PREFIX.trim_end_matches('/')
    }

    /// The stream today's deliveries are appended to.
    #[must_use]
    pub fn current_day_file(&self) -> String {
        day_stream(&utc_day())
    }

    /// Append one delivery and return the entry as recorded.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Archive`] if the entry cannot be serialized or the
    /// stream cannot be appended to.
    pub async fn record(
        &self,
        toolkit: &str,
        trigger: &str,
        metadata_id: &str,
        metadata_uuid: &str,
        payload: &serde_json::Value,
    ) -> Result<ComposioTriggerHistoryEntry> {
        let entry = ComposioTriggerHistoryEntry {
            received_at_ms: now_ms(),
            toolkit: toolkit.to_string(),
            trigger: trigger.to_string(),
            metadata_id: metadata_id.to_string(),
            metadata_uuid: metadata_uuid.to_string(),
            payload: payload.clone(),
        };

        let stream = self.current_day_file();
        let failed = |message: String| Error::Archive {
            path: stream.clone(),
            message,
        };
        let value = serde_json::to_value(&entry)
            .map_err(|error| failed(format!("could not serialize the delivery: {error}")))?;

        // The port assigns the offset atomically, so two deliveries arriving at
        // once land as two whole records with no locking here.
        let offset = self
            .streams
            .append(&stream, value)
            .await
            .map_err(|error| failed(format!("could not append to the archive: {error}")))?;

        tracing::debug!(
            toolkit = %entry.toolkit,
            trigger = %entry.trigger,
            metadata_id = %entry.metadata_id,
            offset,
            "[connectors][triggers] delivery archived"
        );
        Ok(entry)
    }

    /// The most recent `limit` deliveries, newest first.
    ///
    /// A record that will not parse is skipped rather than failing the read:
    /// one corrupt record must not hide every other delivery the user is trying
    /// to look at.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Archive`] when the archive cannot be listed or read.
    pub async fn list_recent(&self, limit: Option<usize>) -> Result<ComposioTriggerHistoryResult> {
        if self.streams.capabilities().contains(Capability::StreamSeal) {
            self.seal_idle_days()
                .await
                .map_err(|error| Error::Archive {
                    path: STREAM_PREFIX.to_owned(),
                    message: format!("could not apply archive retention: {error}"),
                })?;
        }
        let limit = limit.unwrap_or(DEFAULT_HISTORY_LIMIT).max(1);
        let failed = |stream: &str, message: String| Error::Archive {
            path: stream.to_string(),
            message,
        };

        let mut day_streams = self.streams.streams(STREAM_PREFIX).await.map_err(|error| {
            failed(
                STREAM_PREFIX,
                format!("could not list the archive: {error}"),
            )
        })?;
        day_streams.extend(
            self.streams
                .streams(LEGACY_STREAM_PREFIX)
                .await
                .map_err(|error| {
                    failed(
                        LEGACY_STREAM_PREFIX,
                        format!("could not list the legacy archive: {error}"),
                    )
                })?,
        );
        // Newest day first: the name ends in a sortable ISO date.
        day_streams.sort_unstable_by(|left, right| right.cmp(left));

        let mut entries: Vec<ComposioTriggerHistoryEntry> = Vec::new();
        let mut seen = HashSet::new();
        for stream in day_streams {
            let length = self.streams.len(&stream).await.map_err(|error| {
                failed(&stream, format!("could not size the day's stream: {error}"))
            })?;
            // Within a day the stream is append-ordered, so newest is last. Read
            // backwards in windows until `wanted` valid entries are found, so a
            // corrupt record does not shrink the result while older valid ones
            // remain.
            let mut stream_entries = 0;
            let mut end = length;
            while end > 0 && stream_entries < limit {
                let need = limit - stream_entries;
                let from = end.saturating_sub(need as u64);
                let records = self
                    .streams
                    .read_window(&stream, from, usize::try_from(end - from).unwrap_or(need))
                    .await
                    .map_err(|error| {
                        failed(&stream, format!("could not read the day's stream: {error}"))
                    })?;
                for record in records.into_iter().rev() {
                    let Ok(entry) =
                        serde_json::from_value::<ComposioTriggerHistoryEntry>(record.value)
                    else {
                        continue;
                    };
                    // Migration copies retain the source offset. Deduplicate
                    // only matching records at the same day and offset across
                    // the legacy and current stream names; metadata values
                    // alone are not unique delivery identifiers.
                    let day = stream
                        .strip_prefix(STREAM_PREFIX)
                        .or_else(|| stream.strip_prefix(LEGACY_STREAM_PREFIX))
                        .unwrap_or(&stream)
                        .to_owned();
                    let key = (
                        day,
                        record.offset,
                        serde_json::to_string(&entry).unwrap_or_default(),
                    );
                    if seen.insert(key) {
                        stream_entries += 1;
                        entries.push(entry);
                    }
                }
                end = from;
            }
        }

        entries.sort_unstable_by_key(|entry| std::cmp::Reverse(entry.received_at_ms));
        entries.truncate(limit);
        Ok(ComposioTriggerHistoryResult {
            archive_dir: self.archive_dir().to_string(),
            current_day_file: self.current_day_file(),
            entries,
        })
    }

    async fn seal_idle_days(&self) -> tinystoragedrivers_types::Result<()> {
        let now = i64::try_from(now_ms()).unwrap_or(i64::MAX);
        for prefix in [STREAM_PREFIX, LEGACY_STREAM_PREFIX] {
            for stream in self.streams.streams(prefix).await? {
                if let Some(info) = self.streams.info(&stream).await?
                    && RETENTION.should_seal(now, info.last_append_ms)
                {
                    self.streams.seal(&stream).await?;
                }
            }
        }
        Ok(())
    }
}

/// An archive day is immutable after its UTC day plus this idle margin.
const DAY_STREAM_IDLE_MS: u64 = 2 * 24 * 60 * 60 * 1000;

/// Connector-owned retention trigger for completed UTC-day streams.
#[derive(Debug, Clone, Copy)]
struct ArchiveRetention {
    idle_ms: u64,
}

const RETENTION: ArchiveRetention = ArchiveRetention {
    idle_ms: DAY_STREAM_IDLE_MS,
};

impl ArchiveRetention {
    fn should_seal(self, now_ms: i64, last_append_ms: Option<i64>) -> bool {
        let idle_ms = i64::try_from(self.idle_ms).unwrap_or(i64::MAX);
        last_append_ms.is_some_and(|last| last <= now_ms.saturating_sub(idle_ms))
    }
}

fn day_stream(day: &str) -> String {
    format!("{STREAM_PREFIX}{day}")
}

/// Milliseconds since the Unix epoch.
///
/// A clock set before 1970 yields zero rather than panicking: a wrong timestamp
/// on an archived delivery is a great deal better than dropping the delivery.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        })
}

/// Today's UTC date as `YYYY-MM-DD`.
///
/// UTC because the name orders the archive, and a local-time boundary would
/// reorder days when a machine changes timezone.
fn utc_day() -> String {
    chrono::Utc::now().format("%Y-%m-%d").to_string()
}

#[cfg(test)]
#[path = "archive_tests.rs"]
mod tests;
