// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::hash_map::Entry;
use std::num::NonZeroU32;
use std::ops::RangeInclusive;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use byte_unit::Byte;
use fail::fail_point;
use fnv::FnvHashMap;
use itertools::Itertools;
use quickwit_actors::{Actor, ActorContext, ActorExitStatus, Handler, Mailbox, QueueCapacity};
use quickwit_common::io::IoControls;
use quickwit_common::runtimes::RuntimeType;
use quickwit_config::IndexingSettings;
use quickwit_doc_mapper::{DocMapper, QUICKWIT_TOKENIZER_MANAGER};
use quickwit_metastore::checkpoint::{IndexCheckpointDelta, SourceCheckpointDelta};
use quickwit_metastore::Metastore;
use serde::Serialize;
use tantivy::schema::Schema;
use tantivy::store::{Compressor, ZstdCompressor};
use tantivy::{DateTime, IndexBuilder, IndexSettings};
use tokio::runtime::Handle;
use tracing::{info, info_span, warn, Span};
use ulid::Ulid;

use crate::actors::IndexSerializer;
use crate::models::{
    CommitTrigger, IndexedSplitBatchBuilder, IndexedSplitBuilder, IndexingPipelineId,
    NewPublishLock, PreparedDoc, PreparedDocBatch, PublishLock, ScratchDirectory,
};

// Random partition id used to gather partitions exceeding the maximum number of partitions.
const OTHER_PARTITION_ID: u64 = 3264326757911759461u64;

#[derive(Debug)]
struct CommitTimeout {
    workbench_id: Ulid,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct IndexerCounters {
    /// Number of splits that were emitted by the indexer.
    pub num_splits_emitted: u64,

    /// Number of split batches that were emitted by the indexer.
    pub num_split_batches_emitted: u64,

    /// Number of (valid) documents in the current workbench.
    /// This value is used to trigger commit and for observation.
    pub num_docs_in_workbench: u64,
}

struct IndexerState {
    pipeline_id: IndexingPipelineId,
    metastore: Arc<dyn Metastore>,
    indexing_directory: ScratchDirectory,
    indexing_settings: IndexingSettings,
    publish_lock: PublishLock,
    schema: Schema,
    max_num_partitions: NonZeroU32,
    index_settings: IndexSettings,
}

impl IndexerState {
    fn create_indexed_split_builder(
        &self,
        partition_id: u64,
        last_delete_opstamp: u64,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<IndexedSplitBuilder> {
        let index_builder = IndexBuilder::new()
            .settings(self.index_settings.clone())
            .schema(self.schema.clone())
            .tokenizers(QUICKWIT_TOKENIZER_MANAGER.clone());

        let io_controls = IoControls::default()
            .set_progress(ctx.progress().clone())
            .set_kill_switch(ctx.kill_switch().clone())
            .set_index_and_component(&self.pipeline_id.index_id, "indexer");

        let indexed_split = IndexedSplitBuilder::new_in_dir(
            self.pipeline_id.clone(),
            partition_id,
            last_delete_opstamp,
            self.indexing_directory.clone(),
            index_builder,
            io_controls,
        )?;
        info!(
            split_id = indexed_split.split_id(),
            partition_id = partition_id,
            "new-split"
        );
        Ok(indexed_split)
    }

    fn get_or_create_indexed_split<'a>(
        &self,
        partition_id: u64,
        last_delete_opstamp: u64,
        splits: &'a mut FnvHashMap<u64, IndexedSplitBuilder>,
        other_split_opt: &'a mut Option<IndexedSplitBuilder>,
        counter: &'a mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexedSplitBuilder> {
        let num_splits = splits.len();
        match splits.entry(partition_id) {
            Entry::Occupied(indexed_split) => Ok(indexed_split.into_mut()),
            Entry::Vacant(vacant_entry) => {
                if num_splits as u32 >= self.max_num_partitions.get() {
                    // In order to avoid exceeding max_num_partitions, we map the document to the
                    // `OTHER` special partition.
                    if other_split_opt.is_none() {
                        warn!(
                            num_docs_in_workbench = counter.num_docs_in_workbench,
                            max_num_partition = self.max_num_partitions.get(),
                            "Exceeding max_num_partition"
                        );
                        let new_other_split = self.create_indexed_split_builder(
                            OTHER_PARTITION_ID,
                            last_delete_opstamp,
                            ctx,
                        )?;
                        *other_split_opt = Some(new_other_split);
                    }
                    Ok(other_split_opt.as_mut().unwrap())
                } else {
                    let indexed_split =
                        self.create_indexed_split_builder(partition_id, last_delete_opstamp, ctx)?;
                    Ok(vacant_entry.insert(indexed_split))
                }
            }
        }
    }

    async fn create_workbench(
        &self,
        ctx: &ActorContext<Indexer>,
    ) -> anyhow::Result<IndexingWorkbench> {
        let last_delete_opstamp = ctx
            .protect_future(
                self.metastore
                    .last_delete_opstamp(&self.pipeline_id.index_id),
            )
            .await?;
        let batch_parent_span = info_span!(target: "quickwit-indexing", "index_batch",
            index_id=%self.pipeline_id.index_id,
            source_id=%self.pipeline_id.source_id,
            pipeline_ord=%self.pipeline_id.pipeline_ord
        );
        let indexing_span = info_span!(parent: batch_parent_span.id(), "indexer");
        let workbench = IndexingWorkbench {
            batch_parent_span,
            _indexing_span: indexing_span,
            workbench_id: Ulid::new(),
            indexed_splits: FnvHashMap::with_capacity_and_hasher(250, Default::default()),
            other_indexed_split_opt: None,
            checkpoint_delta: IndexCheckpointDelta {
                source_id: self.pipeline_id.source_id.clone(),
                source_delta: SourceCheckpointDelta::default(),
            },
            publish_lock: self.publish_lock.clone(),
            last_delete_opstamp,
            memory_usage: Byte::from_bytes(0),
        };
        Ok(workbench)
    }

    /// Returns the current_indexed_split. If this is the first message, then
    /// the indexed_split does not exist yet.
    ///
    /// This function will then create it, and can hence return an Error.
    async fn get_or_create_workbench<'a>(
        &'a self,
        indexing_workbench_opt: &'a mut Option<IndexingWorkbench>,
        ctx: &'a ActorContext<Indexer>,
    ) -> anyhow::Result<&'a mut IndexingWorkbench> {
        if indexing_workbench_opt.is_none() {
            let indexing_workbench = self.create_workbench(ctx).await?;
            let commit_timeout_message = CommitTimeout {
                workbench_id: indexing_workbench.workbench_id,
            };
            ctx.schedule_self_msg(
                self.indexing_settings.commit_timeout(),
                commit_timeout_message,
            )
            .await;
            *indexing_workbench_opt = Some(indexing_workbench);
        }
        let current_indexing_workbench = indexing_workbench_opt.as_mut().context(
            "No index writer available. This should never happen! Please, report on https://github.com/quickwit-oss/quickwit/issues."
        )?;
        Ok(current_indexing_workbench)
    }

    async fn index_batch(
        &self,
        batch: PreparedDocBatch,
        indexing_workbench_opt: &mut Option<IndexingWorkbench>,
        counters: &mut IndexerCounters,
        ctx: &ActorContext<Indexer>,
    ) -> Result<(), ActorExitStatus> {
        let IndexingWorkbench {
            checkpoint_delta,
            indexed_splits,
            other_indexed_split_opt,
            publish_lock,
            last_delete_opstamp,
            memory_usage,
            ..
        } = self
            .get_or_create_workbench(indexing_workbench_opt, ctx)
            .await?;
        if publish_lock.is_dead() {
            return Ok(());
        }
        checkpoint_delta
            .source_delta
            .extend(batch.checkpoint_delta)
            .context("Batch delta does not follow indexer checkpoint")?;
        let mut memory_usage_delta: u64 = 0;
        for doc in batch.docs {
            let PreparedDoc {
                doc,
                timestamp_opt,
                partition,
                num_bytes,
            } = doc;
            counters.num_docs_in_workbench += 1;
            let indexed_split: &mut IndexedSplitBuilder = self.get_or_create_indexed_split(
                partition,
                *last_delete_opstamp,
                indexed_splits,
                other_indexed_split_opt,
                counters,
                ctx,
            )?;
            let mem_usage_before = indexed_split.index_writer.mem_usage() as u64;
            indexed_split.split_attrs.uncompressed_docs_size_in_bytes += num_bytes as u64;
            indexed_split.split_attrs.num_docs += 1;
            if let Some(timestamp) = timestamp_opt {
                record_timestamp(timestamp, &mut indexed_split.split_attrs.time_range);
            }
            let _protect_guard = ctx.protect_zone();
            indexed_split
                .index_writer
                .add_document(doc)
                .context("Failed to add document.")?;
            let mem_usage_after = indexed_split.index_writer.mem_usage() as u64;
            memory_usage_delta += mem_usage_after - mem_usage_before;
            ctx.record_progress();
        }
        *memory_usage = Byte::from_bytes(memory_usage.get_bytes() + memory_usage_delta);
        Ok(())
    }
}

/// A workbench hosts the set of `IndexedSplit` that are being built.
struct IndexingWorkbench {
    workbench_id: Ulid,
    // This span is used for the entire lifetime of the splits batch creations
    // This span is meant to be passed through the pipeline.
    batch_parent_span: Span,
    // Span for the in-memory indexing (done in the Indexer actor).
    _indexing_span: Span,

    indexed_splits: FnvHashMap<u64, IndexedSplitBuilder>,
    other_indexed_split_opt: Option<IndexedSplitBuilder>,

    checkpoint_delta: IndexCheckpointDelta,
    publish_lock: PublishLock,
    // On workbench creation, we fetch from the metastore the last delete task opstamp.
    // We use this value to set the `delete_opstamp` of the workbench splits.
    last_delete_opstamp: u64,
    // Number of bytes declared as used by tantivy.
    memory_usage: Byte,
}

pub struct Indexer {
    indexer_state: IndexerState,
    index_serializer_mailbox: Mailbox<IndexSerializer>,
    indexing_workbench_opt: Option<IndexingWorkbench>,
    metastore: Arc<dyn Metastore>,
    counters: IndexerCounters,
}

#[async_trait]
impl Actor for Indexer {
    type ObservableState = IndexerCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(10)
    }

    fn name(&self) -> String {
        "Indexer".to_string()
    }

    fn runtime_handle(&self) -> Handle {
        RuntimeType::Blocking.get_runtime_handle()
    }

    #[inline]
    fn yield_after_each_message(&self) -> bool {
        false
    }

    async fn finalize(
        &mut self,
        exit_status: &ActorExitStatus,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        match exit_status {
            ActorExitStatus::DownstreamClosed
            | ActorExitStatus::Killed
            | ActorExitStatus::Failure(_)
            | ActorExitStatus::Panicked => return Ok(()),
            ActorExitStatus::Quit | ActorExitStatus::Success => {
                self.send_to_serializer(CommitTrigger::NoMoreDocs, ctx)
                    .await?;
            }
        }
        Ok(())
    }
}

fn record_timestamp(timestamp: DateTime, time_range: &mut Option<RangeInclusive<DateTime>>) {
    let new_timestamp_range = match time_range {
        Some(range) => timestamp.min(*range.start())..=timestamp.max(*range.end()),
        None => timestamp..=timestamp,
    };
    *time_range = Some(new_timestamp_range);
}

#[async_trait]
impl Handler<CommitTimeout> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        commit_timeout: CommitTimeout,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(indexing_workbench) = &self.indexing_workbench_opt {
            // If this is a timeout for a different workbench, we must ignore it.
            if indexing_workbench.workbench_id != commit_timeout.workbench_id {
                return Ok(());
            }
        }
        self.send_to_serializer(CommitTrigger::Timeout, ctx).await?;
        Ok(())
    }
}

#[async_trait]
impl Handler<PreparedDocBatch> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        doc_batch: PreparedDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.index_batch(doc_batch, ctx).await
    }
}

#[async_trait]
impl Handler<NewPublishLock> for Indexer {
    type Reply = ();

    async fn handle(
        &mut self,
        message: NewPublishLock,
        _ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        let NewPublishLock(publish_lock) = message;
        self.indexing_workbench_opt = None;
        self.indexer_state.publish_lock = publish_lock;
        Ok(())
    }
}

impl Indexer {
    pub fn new(
        pipeline_id: IndexingPipelineId,
        doc_mapper: Arc<dyn DocMapper>,
        metastore: Arc<dyn Metastore>,
        indexing_directory: ScratchDirectory,
        indexing_settings: IndexingSettings,
        index_serializer_mailbox: Mailbox<IndexSerializer>,
    ) -> Self {
        let schema = doc_mapper.schema();
        let docstore_compression = Compressor::Zstd(ZstdCompressor {
            compression_level: Some(indexing_settings.docstore_compression_level),
        });
        let index_settings = IndexSettings {
            docstore_blocksize: indexing_settings.docstore_blocksize,
            docstore_compression,
            docstore_compress_dedicated_thread: true,
            sort_by_field: None,
        };
        let publish_lock = PublishLock::default();
        Self {
            indexer_state: IndexerState {
                pipeline_id,
                metastore: metastore.clone(),
                indexing_directory,
                indexing_settings,
                publish_lock,
                schema,
                index_settings,
                max_num_partitions: doc_mapper.max_num_partitions(),
            },
            index_serializer_mailbox,
            indexing_workbench_opt: None,
            metastore,
            counters: IndexerCounters::default(),
        }
    }

    fn memory_usage(&self) -> Byte {
        if let Some(workbench) = &self.indexing_workbench_opt {
            workbench.memory_usage
        } else {
            Byte::from_bytes(0)
        }
    }

    async fn index_batch(
        &mut self,
        batch: PreparedDocBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        fail_point!("indexer:batch:before");
        self.indexer_state
            .index_batch(
                batch,
                &mut self.indexing_workbench_opt,
                &mut self.counters,
                ctx,
            )
            .await?;
        if self.memory_usage() >= self.indexer_state.indexing_settings.resources.heap_size {
            self.send_to_serializer(CommitTrigger::MemoryLimit, ctx)
                .await?;
        }
        if self.counters.num_docs_in_workbench
            >= self.indexer_state.indexing_settings.split_num_docs_target as u64
        {
            self.send_to_serializer(CommitTrigger::NumDocsLimit, ctx)
                .await?;
        }
        fail_point!("indexer:batch:after");
        Ok(())
    }

    /// Extract the indexed split and send it to the IndexSerializer.
    async fn send_to_serializer(
        &mut self,
        commit_trigger: CommitTrigger,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<()> {
        let IndexingWorkbench {
            indexed_splits,
            other_indexed_split_opt,
            checkpoint_delta,
            publish_lock,
            batch_parent_span,
            ..
        } = if let Some(indexing_workbench) = self.indexing_workbench_opt.take() {
            indexing_workbench
        } else {
            return Ok(());
        };

        let mut splits: Vec<IndexedSplitBuilder> = indexed_splits.into_values().collect();
        if let Some(other_split) = other_indexed_split_opt {
            splits.push(other_split)
        }

        // Avoid producing empty split, but still update the checkpoint to avoid
        // reprocessing the same faulty documents.
        if splits.is_empty() {
            if let Some(_guard) = publish_lock.acquire().await {
                ctx.protect_future(self.metastore.publish_splits(
                    &self.indexer_state.pipeline_id.index_id,
                    &[],
                    &[],
                    Some(checkpoint_delta),
                ))
                .await
                .with_context(|| {
                    format!(
                        "Failed to update the checkpoint for {}, {} after a split containing only \
                         errors.",
                        &self.indexer_state.pipeline_id.index_id,
                        &self.indexer_state.pipeline_id.source_id
                    )
                })?;
            } else {
                info!(
                    split_ids=?splits.iter().map(|split| split.split_id()).join(", "),
                    "Splits' publish lock is dead."
                );
                // TODO: Remove the junk right away?
            }
            return Ok(());
        }
        let num_splits = splits.len() as u64;
        let split_ids = splits.iter().map(|split| split.split_id()).join(",");
        info!(commit_trigger=?commit_trigger, split_ids=%split_ids, num_docs=self.counters.num_docs_in_workbench, "send-to-index-serializer");
        ctx.send_message(
            &self.index_serializer_mailbox,
            IndexedSplitBatchBuilder {
                batch_parent_span,
                splits,
                checkpoint_delta: Some(checkpoint_delta),
                publish_lock,
                commit_trigger,
            },
        )
        .await?;
        self.counters.num_docs_in_workbench = 0;
        self.counters.num_splits_emitted += num_splits;
        self.counters.num_split_batches_emitted += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Write;
    use std::sync::Arc;
    use std::time::Duration;

    use quickwit_actors::Universe;
    use quickwit_doc_mapper::{default_doc_mapper_for_test, DefaultDocMapper};
    use quickwit_metastore::checkpoint::SourceCheckpointDelta;
    use quickwit_metastore::MockMetastore;
    use tantivy::{doc, DateTime};

    use super::*;
    use crate::actors::indexer::{record_timestamp, IndexerCounters};
    use crate::models::ScratchDirectory;

    #[test]
    fn test_record_timestamp() {
        let mut time_range = None;
        record_timestamp(DateTime::from_timestamp_secs(1628664679), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664679)
                    ..=DateTime::from_timestamp_secs(1628664679)
            )
        );
        record_timestamp(DateTime::from_timestamp_secs(1628664112), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664112)
                    ..=DateTime::from_timestamp_secs(1628664679)
            )
        );
        record_timestamp(DateTime::from_timestamp_secs(1628665112), &mut time_range);
        assert_eq!(
            time_range,
            Some(
                DateTime::from_timestamp_secs(1628664112)
                    ..=DateTime::from_timestamp_secs(1628665112)
            )
        )
    }

    #[tokio::test]
    async fn test_indexer_trigger_on_target_num_docs() -> anyhow::Result<()> {
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 3;
        let universe = Universe::with_accelerated_time();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .times(2)
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(last_delete_opstamp)
            });
        metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"this is a test document",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 2",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                ],
                checkpoint_delta: SourceCheckpointDelta::from(4..6),
            })
            .await?;
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 3",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435i64)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435i64)),
                        partition: 1,
                        num_bytes: 30,
                    },
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"this is a test document 4",
                            timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                        ),
                        timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                        partition: 1,
                        num_bytes: 30,
                    },
                ],
                checkpoint_delta: SourceCheckpointDelta::from(6..8),
            })
            .await?;
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![PreparedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                checkpoint_delta: SourceCheckpointDelta::from(8..9),
            })
            .await?;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 1, //< the num docs in split counter has been reset.
            }
        );
        let messages: Vec<IndexedSplitBatchBuilder> = index_serializer_inbox.drain_for_test_typed();
        assert_eq!(messages.len(), 1);
        let batch = messages.into_iter().next().unwrap();
        assert_eq!(batch.commit_trigger, CommitTrigger::NumDocsLimit);
        assert_eq!(batch.splits[0].split_attrs.num_docs, 4);
        assert_eq!(
            batch.splits[0].split_attrs.delete_opstamp,
            last_delete_opstamp
        );
        let index_checkpoint = batch.checkpoint_delta.unwrap();
        assert_eq!(index_checkpoint.source_id, "test-source");
        assert_eq!(
            index_checkpoint.source_delta,
            SourceCheckpointDelta::from(4..8)
        );
        let first_split = batch.splits.into_iter().next().unwrap().finalize()?;
        assert!(first_split.index.settings().sort_by_field.is_none());
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_trigger_on_memory_limit() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.resources.heap_size = Byte::from_bytes(5_000_000);
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .times(1..=2)
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(last_delete_opstamp)
            });
        metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, _indexer_handle) = universe.spawn_builder().spawn(indexer);

        let make_doc = |i: u64| {
            let mut body = String::new();
            for val in 100 * i..100 * (i + 1) {
                write!(&mut body, "{val} ").unwrap();
            }
            let num_bytes = body.len() * 2;
            PreparedDoc {
                doc: doc!(body_field=>body),
                timestamp_opt: None,
                partition: 0,
                num_bytes,
            }
        };
        for i in 0..10_000 {
            indexer_mailbox
                .send_message(PreparedDocBatch {
                    docs: vec![make_doc(i)],
                    checkpoint_delta: SourceCheckpointDelta::from(i..i + 1),
                })
                .await?;
            let output_messages: Vec<IndexedSplitBatchBuilder> =
                index_serializer_inbox.drain_for_test_typed();
            if !output_messages.is_empty() {
                assert_eq!(output_messages.len(), 1);
                assert_eq!(
                    output_messages[0].commit_trigger,
                    CommitTrigger::MemoryLimit
                );
                // The following assert is not a strict one. It should help detect large
                // regression in memory usage.
                assert!((500..1_000).contains(&i));
                break;
            }
        }
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_on_timeout() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let last_delete_opstamp = 10;
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .once()
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(last_delete_opstamp)
            });
        metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![PreparedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=>DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                checkpoint_delta: SourceCheckpointDelta::from(8..9),
            })
            .await
            .unwrap();
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
                num_docs_in_workbench: 1,
            }
        );
        universe.sleep(Duration::from_secs(61)).await;
        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
            }
        );
        let indexed_split_batches: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(indexed_split_batches.len(), 1);
        assert_eq!(
            indexed_split_batches[0].commit_trigger,
            CommitTrigger::Timeout
        );
        assert_eq!(indexed_split_batches[0].splits[0].split_attrs.num_docs, 1);
        assert_eq!(
            indexed_split_batches[0].splits[0]
                .split_attrs
                .delete_opstamp,
            last_delete_opstamp
        );
        universe.assert_quit().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_indexer_eof() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper = Arc::new(default_doc_mapper_for_test());
        let schema = doc_mapper.schema();
        let body_field = schema.get_field("body").unwrap();
        let timestamp_field = schema.get_field("timestamp").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .once()
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(10)
            });
        metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![PreparedDoc {
                    doc: doc!(
                        body_field=>"this is a test document 5",
                        timestamp_field=> DateTime::from_timestamp_secs(1_662_529_435)
                    ),
                    timestamp_opt: Some(DateTime::from_timestamp_secs(1_662_529_435)),
                    partition: 1,
                    num_bytes: 30,
                }],
                checkpoint_delta: SourceCheckpointDelta::from(8..9),
            })
            .await
            .unwrap();
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(exit_status.is_success());
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_splits_emitted: 1,
                num_split_batches_emitted: 1,
                num_docs_in_workbench: 0,
            }
        );
        let output_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(output_messages.len(), 1);
        assert_eq!(output_messages[0].commit_trigger, CommitTrigger::NoMoreDocs);
        assert_eq!(output_messages[0].splits[0].split_attrs.num_docs, 1);
        universe.assert_quit().await;
        Ok(())
    }

    const DOCMAPPER_WITH_PARTITION_JSON: &str = r#"{
        "tag_fields": ["tenant"],
        "partition_key": "tenant",
        "field_mappings": [
            { "name": "tenant", "type": "text", "tokenizer": "raw", "indexed": true },
            { "name": "body", "type": "text" }
        ]
    }"#;

    #[tokio::test]
    async fn test_indexer_partitioning() -> anyhow::Result<()> {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper: Arc<dyn DocMapper> = Arc::new(
            serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_WITH_PARTITION_JSON).unwrap(),
        );
        let schema = doc_mapper.schema();
        let tenant_field = schema.get_field("tenant").unwrap();
        let body_field = schema.get_field("body").unwrap();

        let indexing_directory = ScratchDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .once()
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(10)
            });
        metastore.expect_publish_splits().never();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"doc 2",
                            tenant_field=>"tenant_1",
                        ),
                        timestamp_opt: None,
                        partition: 1,
                        num_bytes: 30,
                    },
                    PreparedDoc {
                        doc: doc!(
                            body_field=>"doc 2",
                            tenant_field=>"tenant_2",
                        ),
                        timestamp_opt: None,
                        partition: 3,
                        num_bytes: 30,
                    },
                ],
                checkpoint_delta: SourceCheckpointDelta::from(8..9),
            })
            .await?;

        let indexer_counters = indexer_handle.process_pending_and_observe().await.state;
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_docs_in_workbench: 2,
                num_splits_emitted: 0,
                num_split_batches_emitted: 0,
            }
        );
        universe.send_exit_with_success(&indexer_mailbox).await?;
        let (exit_status, indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));
        assert_eq!(
            indexer_counters,
            IndexerCounters {
                num_docs_in_workbench: 0,
                num_splits_emitted: 2,
                num_split_batches_emitted: 1,
            }
        );
        let split_batches: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(split_batches.len(), 1);
        assert_eq!(split_batches[0].splits.len(), 2);
        universe.assert_quit().await;
        Ok(())
    }

    const DOCMAPPER_SIMPLE_JSON: &str = r#"{
        "field_mappings": [{"name": "body", "type": "text"}],
        "max_num_partitions": 10
    }"#;

    #[tokio::test]
    async fn test_indexer_exceeding_max_num_partitions() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper: Arc<dyn DocMapper> =
            Arc::new(serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let indexing_settings = IndexingSettings::for_test();
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .times(1)
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(10)
            });
        metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        for partition in 0..100 {
            indexer_mailbox
                .send_message(PreparedDocBatch {
                    docs: vec![PreparedDoc {
                        doc: doc!(body_field=>"doc {i}"),
                        timestamp_opt: None,
                        partition,
                        num_bytes: 30,
                    }],
                    checkpoint_delta: SourceCheckpointDelta::from(partition..partition + 1),
                })
                .await
                .unwrap();
        }
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();

        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_msgs: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(index_serializer_msgs.len(), 1);
        let msg = index_serializer_msgs.into_iter().next().unwrap();
        assert_eq!(msg.splits.len(), 11);
        for split in msg.splits {
            if split.split_attrs.partition_id == OTHER_PARTITION_ID {
                assert_eq!(split.split_attrs.num_docs, 90);
            } else {
                assert_eq!(split.split_attrs.num_docs, 1);
            }
        }
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_propagates_publish_lock() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper: Arc<dyn DocMapper> =
            Arc::new(serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 1;
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .times(2)
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(10)
            });
        metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        let first_lock = PublishLock::default();
        let second_lock = PublishLock::default();

        for lock in [&first_lock, &second_lock] {
            indexer_mailbox
                .send_message(NewPublishLock(lock.clone()))
                .await
                .unwrap();
            indexer_mailbox
                .send_message(PreparedDocBatch {
                    docs: vec![PreparedDoc {
                        doc: doc!(body_field=>"doc 1"),
                        timestamp_opt: None,
                        partition: 0,
                        num_bytes: 30,
                    }],
                    checkpoint_delta: SourceCheckpointDelta::from(0..1),
                })
                .await
                .unwrap();
        }
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_messages: Vec<IndexedSplitBatchBuilder> =
            index_serializer_inbox.drain_for_test_typed();
        assert_eq!(index_serializer_messages.len(), 2);
        assert_eq!(index_serializer_messages[0].splits.len(), 1);
        assert_eq!(index_serializer_messages[0].publish_lock, first_lock);
        assert_eq!(index_serializer_messages[1].splits.len(), 1);
        assert_eq!(index_serializer_messages[1].publish_lock, second_lock);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexer_ignores_messages_when_publish_lock_is_dead() {
        let universe = Universe::with_accelerated_time();
        let pipeline_id = IndexingPipelineId {
            index_id: "test-index".to_string(),
            source_id: "test-source".to_string(),
            node_id: "test-node".to_string(),
            pipeline_ord: 0,
        };
        let doc_mapper: Arc<dyn DocMapper> =
            Arc::new(serde_json::from_str::<DefaultDocMapper>(DOCMAPPER_SIMPLE_JSON).unwrap());
        let body_field = doc_mapper.schema().get_field("body").unwrap();
        let indexing_directory = ScratchDirectory::for_test();
        let mut indexing_settings = IndexingSettings::for_test();
        indexing_settings.split_num_docs_target = 1;
        let mut metastore = MockMetastore::default();
        metastore
            .expect_last_delete_opstamp()
            .times(1)
            .returning(move |index_id| {
                assert_eq!("test-index", index_id);
                Ok(10)
            });
        metastore.expect_publish_splits().never();
        let (index_serializer_mailbox, index_serializer_inbox) = universe.create_test_mailbox();
        let indexer = Indexer::new(
            pipeline_id,
            doc_mapper,
            Arc::new(metastore),
            indexing_directory,
            indexing_settings,
            index_serializer_mailbox,
        );
        let (indexer_mailbox, indexer_handle) = universe.spawn_builder().spawn(indexer);

        let publish_lock = PublishLock::default();
        indexer_mailbox
            .send_message(NewPublishLock(publish_lock.clone()))
            .await
            .unwrap();
        indexer_handle.process_pending_and_observe().await;
        publish_lock.kill().await;
        indexer_mailbox
            .send_message(PreparedDocBatch {
                docs: vec![PreparedDoc {
                    doc: doc!(body_field=>"doc 1"),
                    timestamp_opt: None,
                    partition: 0,
                    num_bytes: 30,
                }],
                checkpoint_delta: SourceCheckpointDelta::from(0..1),
            })
            .await
            .unwrap();
        universe
            .send_exit_with_success(&indexer_mailbox)
            .await
            .unwrap();
        let (exit_status, _indexer_counters) = indexer_handle.join().await;
        assert!(matches!(exit_status, ActorExitStatus::Success));

        let index_serializer_messages = index_serializer_inbox.drain_for_test();
        assert!(index_serializer_messages.is_empty());
        universe.assert_quit().await;
    }
}
