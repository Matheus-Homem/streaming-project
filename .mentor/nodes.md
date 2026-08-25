# Knowledge nodes

<!-- The registry of every taxonomy node this project has touched, and the
     Application classification of each. THE SKILL OWNS THIS FILE.

     One row per node, depth 4 only. Nodes shallower than 4 do not appear here:
     they carry no Application and exist only implicitly, as prefixes.

     application: practical | theoretical — the NATURE of the node, not the
       user's history with it. Is there a class of artifact whose production
       would directly demonstrate this node?
     source:      derived | user — `user` is permanent and is never re-derived.
     why:         one line justifying the classification, written at the same
                  moment as the value. Required.
     origin:      where the node came from — a task id, a design section, or `class`.
     aliases:     comma-separated spellings the user actually types.
     related:     comma-separated cross-tree links. NAVIGATION ONLY —
                  the task matrix never reads this column.

     Application is derived once, on creation, and does not drift.
     Re-derive deliberately with /mentor-map --rederive <node>.
     See references/knowledge-model.md. -->

| node | application | source | why | origin | first_seen | aliases | related |
|---|---|---|---|---|---|---|---|
| StreamProcessing.ApacheFlink.TableApiSql.DynamicTablesVsDataStream | theoretical | derived | Mental-model shift (a stream seen as a table that only ever grows) - demonstrated by explaining or deciding in a scenario, not by producing one specific line of config | class | 2026-08-24 | table api, dynamic table, tabela dinamica, table api vs datastream | StreamProcessing.ApacheFlink.TableApiSql.AppendOnlyVsRetractStreams |
| StreamProcessing.ApacheFlink.TableApiSql.AppendOnlyVsRetractStreams | practical | derived | Resolves to a concrete config choice - the `kafka` connector (append-only) vs. `upsert-kafka` (retract) in a `CREATE TABLE`'s `WITH` clause | class | 2026-08-24 | append-only vs retract, changelog stream, upsert-kafka | StreamProcessing.ApacheFlink.TableApiSql.DynamicTablesVsDataStream |
| StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks | practical | derived | Resolves to a concrete config choice - the `WATERMARK FOR col AS col - INTERVAL 'n' SECOND` clause and its bound | class | 2026-08-24 | watermark, bounded out-of-orderness, atraso limitado, event time | StreamProcessing.ApacheFlink.Windowing.TumblingWindowViaTvf |
| StreamProcessing.ApacheFlink.Windowing.TumblingWindowViaTvf | practical | derived | Resolves to a concrete piece of SQL syntax - `TABLE(TUMBLE(TABLE t, DESCRIPTOR(col), INTERVAL 'n' MINUTES))` and its `GROUP BY` | class | 2026-08-24 | tumbling window, janela tumbling, windowing tvf, TUMBLE | StreamProcessing.ApacheFlink.EventTime.BoundedOutOfOrdernessWatermarks |
| StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend | practical | derived | Resolves to a concrete config choice - checkpoint interval and state backend selection | class | 2026-08-24 | checkpointing, state backend, checkpoint, tolerancia a falhas | StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions |
| StreamProcessing.ApacheFlink.FaultTolerance.ExactlyOnceSinkViaKafkaTransactions | practical | derived | Resolves to a concrete config choice - `sink.delivery-guarantee`/`sink.transactional-id-prefix`/`transaction.timeout.ms` on the Kafka sink, checked against the broker's `transaction.max.timeout.ms` | class | 2026-08-24 | exactly-once, exactly once kafka, transacao kafka, two-phase commit | StreamProcessing.ApacheFlink.FaultTolerance.CheckpointingAndStateBackend |
