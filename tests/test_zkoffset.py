#coding=utf-8

import pytest

from kafkardd.offset_manager import ZKOffsetManager

msg_test_offset = 158

@pytest.fixture(scope='session')
def zk_offset_manager(zk_host, zk_prefix, zk_user, kafka_topic, kafka_partition_count):
    return ZKOffsetManager({
            'hosts': zk_host,
            'prefix': zk_prefix,
            'user': zk_user,
            'topic': kafka_topic
        },
        range(0, kafka_partition_count))

def test_offset_operation(zk_offset_manager, kafka_partition_count):
    offsets = {p: msg_test_offset + p for p in range(0, kafka_partition_count)}
    zk_offset_manager.set_offsets(offsets)
    commited_offsets = zk_offset_manager.get_offsets()
    for p in range(0, kafka_partition_count):
        assert commited_offsets[p] == msg_test_offset + p

