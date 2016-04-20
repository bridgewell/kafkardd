#coding=utf-8

import pytest

from kafkardd.kafkardd import KafkaRDDManager, OffsetPolicy
from testutil import extract_timestamp_from_message

msg_test_offset = 158
msg_test_timestamp = 258

@pytest.fixture(scope='session')
def kafkardd_manager(request, spark_context, kafka_host_str, kafka_topic, zk_host, zk_prefix):
    kafka_rdd_config = {
            'spark_context': spark_context,
            'chunk_size': 0,
            'parallelism': 0,
            'start_policy': OffsetPolicy('earliest'),
            'end_policy': OffsetPolicy('latest'),
            'kafka': {
                'hosts': kafka_host_str,
                'topic': kafka_topic,
                'timestamp_extractor': extract_timestamp_from_message
            },
            'zookeeper': {
                'hosts': zk_host,
                'znode': zk_prefix
            }
        }
    kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
    return kafka_rdd_manager

def test_kafka_offset_range(kafkardd_manager, kafka_partition_count, kafka_msg_count):
    offset_ranges = kafkardd_manager.get_offset_ranges_by_policy(OffsetPolicy('earliest'), OffsetPolicy('latest'))
    for p in range(0, kafka_partition_count):
        assert offset_ranges[p] == (0, kafka_msg_count)

def test_kafka_offset_range_ts(kafkardd_manager, zk_offset_manager, kafka_partition_count, kafka_msg_count):
    offsets = {p: msg_test_offset + p for p in range(0, kafka_partition_count)}
    zk_offset_manager.set_offsets(offsets)
    offset_ranges = kafkardd_manager.get_offset_ranges_by_policy(
                        OffsetPolicy('committed'),
                        OffsetPolicy('timestamp', msg_test_timestamp)
                    )
    for p in range(0, kafka_partition_count):
        assert offset_ranges[p] == (msg_test_offset + p, msg_test_timestamp)

def test_kafka_msg_processor(kafkardd_manager, kafka_partition_count, kafka_msg_count):
    kafkardd_manager.set_offset_ranges_by_policy(
                        OffsetPolicy('committed'),
                        OffsetPolicy('timestamp', msg_test_timestamp)
    )
    def msg_processor(rdd):
        count = rdd.count()
        assert count == (kafka_partition_count
                         * (msg_test_timestamp - msg_test_offset)
                         - sum(range(0, kafka_partition_count)))
    kafkardd_manager.process(msg_processor, commit_policy='after')

def test_stop(kafkardd_manager):
    kafkardd_manager.stop()
    assert True
