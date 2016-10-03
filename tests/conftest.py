#coding=utf-8

import os
import sys

SPARK_HOME = os.path.expanduser(getattr(os.environ, 'SPARK_HOME', '~/Tool/spark-1.5.2-bin-hadoop2.6/'))
KAFKA_HOME = os.path.expanduser(getattr(os.environ, 'KAFKA_HOME', '~/Tool/kafka_2.10-0.8.2.1/'))

if not os.path.isdir(SPARK_HOME):
    raise Exception('SPARK_HOME \"%s\" not exists' % SPARK_HOME)
if not os.path.isdir(KAFKA_HOME):
    raise Exception('KAFKA_HOME \"%s\" not exists' % KAFKA_HOME)

sys.path.append(os.path.join(SPARK_HOME, 'python'))
sys.path.append(os.path.join(SPARK_HOME, 'python', 'lib', 'py4j-0.8.2.1-src.zip'))
os.environ['PYSPARK_SUBMIT_ARGS'] = ' '.join([
    '--packages',
    'org.apache.spark:spark-streaming-kafka_2.10:1.5.2',
    'pyspark-shell'])

from time import sleep
from subprocess import call
import pytest

from kafka import KafkaProducer
from kafkardd.kafkaoffset import KafkaOffsetManager
from kafkardd.zkoffset import ZKOffsetManager

from testutil import generate_message

def pytest_addoption(parser):
    parser.addoption('--zk_host', action='store',
                        default='localhost:2181',
                        help='zookeeper host')
    parser.addoption('--kafka_host', action='store',
                        default='localhost:9092',
                        help='kafka host')
    parser.addoption('--kafka_tool', action='store',
                        default=os.path.join(KAFKA_HOME, 'bin', 'kafka-topics.sh'),
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
    hosts = kafka_host_str.split(',')
    producer = KafkaProducer(bootstrap_servers=hosts)
    for p in range(0, kafka_partition_count):
        for o in range(0, kafka_msg_count):
            f = producer.send(
                    kafka_topic,
                    value=generate_message(p, o, o),
                    partition=p)
        producer.flush()
    request.addfinalizer(kafka_server_stop)
    return hosts

@pytest.fixture(scope='session')
def kafka_topic(request):
    return 'test_topic'

@pytest.fixture(scope='session')
def kafka_partition_count(request):
    return 3

@pytest.fixture(scope='session')
def kafka_msg_count(request):
    return 300

@pytest.fixture(scope='session')
def zk_host(request):
    return request.config.getoption('--zk_host')

@pytest.fixture(scope='session')
def zk_prefix():
    return '/path/to/node'

@pytest.fixture(scope='session')
def zk_offset_manager(request, zk_host, zk_prefix, kafka_partition_count):
    zk = ZKOffsetManager({
            'hosts': zk_host,
            'znode': zk_prefix
        },
        range(0, kafka_partition_count))
    #request.addfinalizer(lambda: zk.stop())
    return zk

@pytest.fixture(scope='session')
def spark_context(request):
    from pyspark import SparkContext
    from pyspark.conf import SparkConf
    conf = (SparkConf()
                .setMaster('local[2]')
                .setAppName('pytest-pyspark-local-testing')
            )
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    return sc

