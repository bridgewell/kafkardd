#coding=utf-8

import pytest

from kafkardd.offset_manager import KafkaOffsetManager

@pytest.fixture(scope='session')
def kafka_offset_manager(kafka_host, kafka_topic):
    return KafkaOffsetManager({'hosts': ','.join(kafka_host), 'topic': kafka_topic})

def test_manager_topic(kafka_offset_manager, kafka_partition_count):
    assert kafka_offset_manager.get_partitions() == range(0, kafka_partition_count)

def test_manager_offset(kafka_offset_manager, kafka_partition_count, kafka_msg_count):
    earliest_offsets = kafka_offset_manager.get_earliest_offsets()
    latest_offsets = kafka_offset_manager.get_latest_offsets()
    for i in range(0, kafka_partition_count):
        assert earliest_offsets[i] == 0
        assert latest_offsets[i] == kafka_msg_count
