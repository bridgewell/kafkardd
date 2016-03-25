#coding=utf-8

import os
import sys

try:
    sys.path.append(os.path.join(
        os.environ['SPARK_HOME'], 'python'))
    sys.path.append(os.path.join(
        os.environ['SPARK_HOME'], 'python', 'lib', 'py4j-0.8.2.1-src.zip'))
except KeyError:
    raise Exception('$SPARK_HOME not set')

from time import sleep
from subprocess import call
import pytest

from kafka import KafkaProducer
from kafkardd.offset_manager import KafkaOffsetManager
from kafkardd.testutil import generate_message

def pytest_addoption(parser):
    parser.addoption('--zk_host', action='store',
                        help='zookeeper host')
    parser.addoption('--kafka_host', action='store',
                        help='kafka host')
    parser.addoption('--kafka_tool', action='store',
                        help='kafka tool for create topic command')


def kafka_server_start(host, port, topic, partition, tool):
    call(['docker', 'run', '-d',
            '-p', '2181:2181',
            '-p', '%s:9092' % port,
            '--env', 'ADVERTISED_HOST=%s' % host,
            '--env', 'ADVERTISED_PORT=%s' % port,
            '--name', 'kafka_test',
            'spotify/kafka'])
    sleep(2)
    call(' '.join([tool, '--create',
            '--zookeeper', '%s:2181' % host,
            '--replication-factor', '1',
            '--partitions', '%s' % partition,
            '--topic', '%s' % topic]), shell=True)

def kafka_server_stop():
    call(['docker', 'kill', 'kafka_test'])
    call(['docker', 'rm', 'kafka_test'])

@pytest.fixture(scope='session')
def kafka_host_str(request):
    return request.config.getoption('--kafka_host')

@pytest.fixture(scope='session')
def kafka_host(request, kafka_host_str, kafka_topic, kafka_partition_count, kafka_msg_count):
    kafka_tool = request.config.getoption('--kafka_tool')
    host, port = kafka_host_str.split(':')
    kafka_server_start(host, port, kafka_topic, kafka_partition_count, kafka_tool)
    request.addfinalizer(kafka_server_stop)
    hosts = kafka_host_str.split(',')
    producer = KafkaProducer(bootstrap_servers=hosts)
    for p in range(0, kafka_partition_count):
        for o in range(0, kafka_msg_count):
            f = producer.send(
                    kafka_topic,
                    value=generate_message(p, o, o),
                    partition=p)
        producer.flush()
    return hosts

@pytest.fixture(scope='session')
def kafka_topic(request):
    return 'test_topic'

@pytest.fixture(scope='session')
def kafka_partition_count(request):
    return 3

@pytest.fixture(scope='session')
def kafka_msg_count(request):
    return 1000

@pytest.fixture(scope='session')
def zk_host(request):
    return request.config.getoption('--zk_host')

@pytest.fixture(scope='session')
def zk_prefix():
    return '/path/to/save'

@pytest.fixture(scope='session')
def zk_user():
    return 'test_user'

@pytest.fixture(scope='session')
def spark_context(request):
    from pyspark import SparkContext
    from pyspark.conf import SparkConf
    conf = (SparkConf()
                .setMaster('local[2]')
                .setAppName('pytest-pyspark-local-testing')
                .set('spark.packages', 'org.apache.spark:spark-streaming-kafka_2.10:1.5.2'))
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    return sc

