#coding=utf-8

import pytest

from kafkardd.offset_manager import KafkaOffsetManager

from testutil import extract_timestamp_from_message

msg_test_offset = 158

@pytest.fixture(scope='session')
def kafka_offset_manager(request, kafka_host, kafka_topic):
    kafka = KafkaOffsetManager({
        'hosts': ','.join(kafka_host),
        'topic': kafka_topic,
        'timestamp_extractor': extract_timestamp_from_message
        })
    #request.addfinalizer(lambda: kafka.stop())
    return kafka

def test_topic(kafka_offset_manager, kafka_partition_count):
    assert kafka_offset_manager.get_partitions() == range(0, kafka_partition_count)

def test_offset_get(kafka_offset_manager, kafka_partition_count, kafka_msg_count):
    earliest_offsets = kafka_offset_manager.get_earliest_offsets()
    latest_offsets = kafka_offset_manager.get_latest_offsets()
    for p in range(0, kafka_partition_count):
        assert earliest_offsets[p] == 0
        assert latest_offsets[p] == kafka_msg_count

def test_offset_get_by_timestamp(kafka_offset_manager, kafka_partition_count):
    offsets = kafka_offset_manager.get_offsets_by_timestamp(msg_test_offset)
    for p in range(0, kafka_partition_count):
        assert offsets[p] == msg_test_offset

def test_timestamp_get_by_offset(kafka_offset_manager, kafka_partition_count):
    offsets = {}
    for p in range(0, kafka_partition_count):
        offsets[p] = msg_test_offset + p
    timestamps = kafka_offset_manager.get_timestamp_by_offsets(offsets)
    for p in range(0, kafka_partition_count):
        assert timestamps[p] == msg_test_offset + p
