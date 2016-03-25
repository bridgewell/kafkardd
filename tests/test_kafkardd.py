#coding=utf-8

import pytest


msg_test_offset = 158

@pytest.fixture(scope='session')
def kafkardd_manager(request, spark_context, kafka_host_str, kafka_topic, zk_host, zk_prefix, zk_user):
    from kafkardd.kafkardd import KafkaRDDManager
    kafka_rdd_config = {
            'spark_context': spark_context,
            'chunk_size': 0,
            'parallelism': 0,
            'start_policy': {'type': 'earliest'},
            'end_policy': {'type': 'latest'},
            'kafka': {
                'hosts': kafka_host_str,
                'topic': kafka_topic
                },
            'zookeeper': {
                'hosts': zk_host,
                'prefix': zk_prefix,
                'user': zk_user,
                'topic': kafka_topic
                }
            }
    kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
    return kafka_rdd_manager

def test_kafka_offsetrange(kafkardd_manager, kafka_partition_count, kafka_msg_count):
    offset_ranges = kafkardd_manager.fetch_offset_ranges_by_policy({'type': 'earliest'}, {'type': 'latest'})
    for p in range(0, kafka_partition_count):
        assert offset_ranges[p] == (0, kafka_msg_count)


