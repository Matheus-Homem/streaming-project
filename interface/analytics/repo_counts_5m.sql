CREATE TABLE normalized_event_envelope (
    partition_key STRING,
    event_time BIGINT,
    event_time_timestamp AS TO_TIMESTAMP_LTZ(event_time, 3),
    WATERMARK FOR event_time_timestamp AS event_time_timestamp - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'events-normalized',
    'properties.bootstrap.servers' = 'broker-1:19092,broker-2:19092,broker-3:19092',
    'properties.group.id' = 'aggregation-consumer-group',
    'properties.auto.offset.reset' = 'earliest',
    'scan.startup.mode' = 'earliest-offset',
    'json.ignore-parse-errors' = 'true',
    'format' = 'json'
);

CREATE TABLE aggregated_event (
    repo_name STRING,
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    event_count BIGINT
) WITH (
    'connector' = 'kafka',
    'topic' = 'events-analytics',
    'properties.bootstrap.servers' = 'broker-1:19092,broker-2:19092,broker-3:19092',
    'key.fields' = 'repo_name',
    'key.format' = 'json',
    'value.format' = 'json',
    'sink.delivery-guarantee' = 'exactly-once',
    'sink.transactional-id-prefix' = 'flink-query-aggregated-events-sink',
    'properties.transaction.timeout.ms' = '600000'
);

INSERT INTO aggregated_event
SELECT
    partition_key,
    window_start,
    window_end,
    COUNT(*) AS event_count
FROM
    TABLE(
        TUMBLE (
            TABLE normalized_event_envelope,
            DESCRIPTOR(event_time_timestamp),
            INTERVAL '5' MINUTE
        )
    )
GROUP BY partition_key, window_start, window_end;
