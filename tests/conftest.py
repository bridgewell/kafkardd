#coding=utf-8

import pytest

from kafka import KafkaProducer
from kafkardd.offset_manager import KafkaOffsetManager

def pytest_addoption(parser):
    parser.addoption('--kafka_host', action='store',
                        help='kafka host')
    parser.addoption('--kafka_topic', action='store',
                        help='kafka topic')
    parser.addoption('--kafka_partition', action='store',
                        help='kafka partition count')

def msg_value_gen(partition, offset):
    return 'p%s_m%s' % (str(partition).zfill(2), str(offset).zfill(6))

@pytest.fixture(scope='session')
def kafka_host(request, kafka_topic, kafka_partition_count, kafka_msg_count):
    hosts = request.config.getoption('--kafka_host').split(',')
    producer = KafkaProducer(bootstrap_servers=hosts)
    for p in range(0, kafka_partition_count):
        for o in range(0, kafka_msg_count):
            f = producer.send(
                    kafka_topic,
                    value=msg_value_gen(p, o),
                    partition=p)
        producer.flush()
    return hosts

@pytest.fixture(scope='session')
def kafka_topic(request):
    return request.config.getoption('--kafka_topic')

@pytest.fixture(scope='session')
def kafka_partition_count(request):
    return int(request.config.getoption('--kafka_partition'))

@pytest.fixture(scope='session')
def kafka_msg_count(request):
    return 1000
