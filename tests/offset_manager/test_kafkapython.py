#coding=utf-8

import pytest

from kafka import KafkaConsumer
from kafka.common import TopicPartition

msg_test_offset = 158

@pytest.fixture(scope='session')
def kafka_consumer(kafka_host, kafka_topic):
    return KafkaConsumer(kafka_topic, bootstrap_servers=kafka_host)

@pytest.fixture
def kafka_consumer_wo_topic(kafka_host):
    return KafkaConsumer(bootstrap_servers=kafka_host)

def msg_value_gen(partition, offset):
    return 'p%s_m%s' % (str(partition).zfill(2), str(offset).zfill(6))

def test_consumer_topic(kafka_consumer, kafka_topic, kafka_partition_count):
    assert kafka_topic in kafka_consumer.topics()
    assginment_list = [TopicPartition(kafka_topic, p) for p in range(0, kafka_partition_count)]
    assert kafka_consumer.assignment() == set(assginment_list)

def test_consumer_offset_seek(kafka_consumer, kafka_topic, kafka_partition_count):
    assert kafka_topic in kafka_consumer.topics()
    p = kafka_partition_count - 1
    kafka_consumer.seek(TopicPartition(kafka_topic, p), msg_test_offset)
    assert kafka_consumer.position(TopicPartition(kafka_topic, p)) == msg_test_offset

def test_consumer_fetch_one(kafka_consumer_wo_topic, kafka_topic, kafka_partition_count):
    p = kafka_partition_count - 1
    kafka_consumer_wo_topic.assign([TopicPartition(kafka_topic, p)])
    kafka_consumer_wo_topic.seek(TopicPartition(kafka_topic, p), msg_test_offset)
    assert kafka_consumer_wo_topic.position(TopicPartition(kafka_topic, p)) == msg_test_offset

    msg = kafka_consumer_wo_topic.next()
    assert msg.value == msg_value_gen(p, msg_test_offset)
    assert msg.topic == kafka_topic
    assert msg.offset == msg_test_offset
    assert msg.partition == p
