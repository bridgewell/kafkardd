#coding=utf-8

import pytest

from kafka import KafkaConsumer
from kafka.common import TopicPartition
from kafkardd.testutil import validate_message

msg_test_offset = 158

@pytest.fixture(scope='session')
def kafka_partition(kafka_partition_count):
    return kafka_partition_count - 1

@pytest.fixture(scope='session')
def kafka_tp(kafka_topic, kafka_partition):
    return TopicPartition(kafka_topic, kafka_partition)

@pytest.fixture(scope='session')
def kafka_consumer_with_topic(kafka_host, kafka_topic):
    return KafkaConsumer(kafka_topic, bootstrap_servers=kafka_host)

@pytest.fixture
def kafka_consumer(kafka_host, kafka_tp):
    consumer = KafkaConsumer(bootstrap_servers=kafka_host)
    consumer.assign([kafka_tp])
    return consumer

def test_consumer_topic(kafka_consumer_with_topic, kafka_topic, kafka_partition_count):
    assert kafka_topic in kafka_consumer_with_topic.topics()
    assginment_list = [TopicPartition(kafka_topic, p) for p in range(0, kafka_partition_count)]
    assert kafka_consumer_with_topic.assignment() == set(assginment_list)

def test_consumer_offset_seek(kafka_consumer, kafka_tp):
    kafka_consumer.seek(kafka_tp, msg_test_offset)
    assert kafka_consumer.position(kafka_tp) == msg_test_offset

def test_consumer_offset_seek_to_beginning(kafka_consumer, kafka_tp):
    kafka_consumer.seek_to_beginning(kafka_tp)
    assert kafka_consumer.position(kafka_tp) == None
    msg = kafka_consumer.next()
    assert msg.offset == 0
    assert kafka_consumer.position(kafka_tp) == 1

def test_consumer_offset_seek_to_end(kafka_consumer, kafka_tp):
    kafka_consumer.seek_to_end(kafka_tp)
    assert kafka_consumer.position(kafka_tp) == None

def test_consumer_fetch_one(kafka_consumer, kafka_tp):
    kafka_consumer.seek(kafka_tp, msg_test_offset)
    assert kafka_consumer.position(kafka_tp) == msg_test_offset
    msg = kafka_consumer.next()
    assert validate_message(msg.value, partition=kafka_tp.partition, offset=msg_test_offset)
    assert msg.topic == kafka_tp.topic
    assert msg.offset == msg_test_offset
    assert msg.partition == kafka_tp.partition
