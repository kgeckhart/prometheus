# Series-less agent prototype notes

Benchmark prototype for the claim: memory is bounded by the segment being written plus the segment each destination is reading. Not upstreamable. Disk mode only.

## What changed

- `tsdb/agent/db.go`, `db_append_v2.go`: no global series table. Appenders return SeriesRef 0. `Commit` (under `segMu`) estimates the batch size, rotates the WAL first if it would not fit, then assigns segment-local refs from a per-segment table and logs series + samples together. Refs restart at 1 in every segment. Float samples are logged in chunks of 4096 so one rotation check covers a bounded amount. Truncation deletes segments up to `WriteStorage.LowestSentSegment()`, never the active one.
- `tsdb/wlog/watcher.go`: `SegmentedWriteTo` (implemented by the queue manager) switches the watcher to `runSegmented`. It starts after the last fully sent segment, calls `SeriesReset(n)` on entering segment n, reads with startTimestamp 0, and calls `SegmentDone(n)` after the segment. The old path is untouched for other writers.
- `tsdb/wlog/wlog.go`: `SegmentAndOffset()` (no directory listing).
- `storage/remote`: `segment_progress.go` (enqueued vs processed counters, `SegmentDone`, `LastSentSegment`, progress file `<data dir>/remote-progress/<queue name>`), `LowestSentSegment` on `WriteStorage` and `Storage`, progress file removed in `ApplyConfig` when a destination disappears. Shards route by labels hash instead of ref.
- Deleted: `tsdb/agent/series.go`, `checkpoint.go` and all their tests, old `db_test.go`, `db_append_v2_test.go`. New `db_test.go` replaces them.

## Borrowed

`storage/remote/series_storage.go` and the queue manager wiring, from `kgeckhart/queue-manager-series-storage-encapsulate` (commits `d5069742e` and `02f4c47a8`, applied with `cherry-pick -n` plus `git apply --3way`; only `queue_manager_test.go` conflicted, resolved by adding the new constructor args). Added a hash to the entries and `LookupHashed` for shard routing.

## Known breakage

- Restart/replay: `replayWAL` is gone. A restart opens a fresh segment with an empty table and the watcher resumes at `progress+1`, resending the partial segment. Not tested.
- "Sent" signal is a cumulative count (items enqueued when the segment finished vs items sent or dropped). A later item can overtake an earlier one across shards and mark a segment early. Items lost in a hard shutdown during resharding never count, which wedges progress until restart.
- Batch size estimate: if it is too low the WAL rotates by itself inside a commit and some samples land in a segment without their series record (counter `prometheus_agent_segment_rotation_misestimates_total`, warning in the log). Those samples are dropped by the queue manager as "not explicitly dropped". A single scrape chunk larger than the segment size will do this.
- Staleness: the scrape loop skips stale markers when the ref is 0 (the separate ref-0 patch in `scrape/` addresses that).
- Exemplars are resolved by the labels passed in, and the latest-exemplar dedupe is gone.
- No `lastTs`: out-of-order and duplicate samples are no longer rejected.
- Metadata: still a no-op in the agent.
- `prometheus_agent_active_series` now means "series in the current segment's table". `prometheus_agent_deleted_series`, `*_checkpoint_*`, `*_corruptions_total`, `*_data_replay_duration_seconds`, `*_out_of_order_samples_total` are removed. New: `prometheus_agent_segment_rotations_total`, `prometheus_agent_segment_rotation_misestimates_total`.
- The watcher still runs its (harmless) checkpoint-GC ticker.
- WAL segments are only deleted on the truncate tick (`--storage.agent.wal-truncate-frequency`, default 2h). No disk cap.
- Ignored options: `StripeSize`, `Min/MaxWALTime`, `OutOfOrderTimeWindow`, checkpoint options.
- Removed-and-readded destination with the same name resumes from its old progress file.
- Only one rotation-on-idle rule: none. A quiet agent keeps its last segment until it fills.

## Verified

Unit tests (agent rotation/refs/truncate, watcher segment reset, queue manager segment progress) plus a throwaway end-to-end run (backend returning 500 while 40k samples were appended, 9 segments piled up on disk with none deleted, then all 40k samples arrived with correct labels and segments before the active one were deleted). Memory flatness itself is not measured here.

## Harness flags

`--agent-flags "--storage.agent.wal-segment-size=10MB --storage.agent.wal-truncate-frequency=10s"`. The size flag is hidden and validated to 10MB..256MB. The default truncate frequency (2h) would never free disk in a short run.
