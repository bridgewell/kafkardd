# -*- coding: utf-8 -*-
import logging

from kafka import KafkaConsumer
from kafka.common import TopicPartition
from kazoo.client import KazooClient

logger = logging.getLogger(__name__)

def _peek_msg_timestamp_by_offset(consumer, offset, topic_partition, timestamp_extractor):
    consumer.seek(topic_partition, offset)
    msg = consumer.next()
    timestamp = timestamp_extractor(msg.value)
    return timestamp


def _bsearch_offset_by_timestamp(consumer, start, end, topic_partition, timestamp, timestamp_extractor):
    if start == end:
        return end
    peek_offset = (start + end) // 2
    t = _peek_msg_timestamp_by_offset(consumer, peek_offset, topic_partition, timestamp_extractor)
    if t < timestamp:
        start = peek_offset + 1
    else:
        end = peek_offset
    offset = _bsearch_offset_by_timestamp(consumer, start, end, topic_partition, timestamp, timestamp_extractor)
    return offset


def _get_partition_offset_by_timestamp(consumer, topic_partition, timestamp, timestamp_extractor):
    start = _get_partition_earliest_offset(consumer, topic_partition)
    end = _get_partition_latest_offset(consumer, topic_partition)
    offset = _bsearch_offset_by_timestamp(consumer, start, end, topic_partition, timestamp, timestamp_extractor)
    return offset


def _get_partition_earliest_offset(consumer, topic_partition):
    consumer.seek_to_beginning(topic_partition)
    consumer._update_fetch_positions([topic_partition])
    return consumer.position(topic_partition)


def _get_partition_latest_offset(consumer, topic_partition):
    consumer.seek_to_end(topic_partition)
    consumer._update_fetch_positions([topic_partition])
    return consumer.position(topic_partition)


def _get_partition_timestamp_by_offset(consumer, topic_partition, offset, timestamp_extractor):
    timestatmp = _peek_msg_timestamp_by_offset(consumer, offset, topic_partition, timestamp_extractor)
    return timestatmp


def _get_partitions_by_topic(hosts, topic):
    consumer = KafkaConsumer(bootstrap_servers=hosts)
    partitions = list(consumer.partitions_for_topic(topic))
    consumer.close()
    return partitions


class KafkaOffsetManager:
    def __init__(self, config):
        self.hosts = config['hosts'].split(',')
        self.topic = config['topic']
        self.partitions = _get_partitions_by_topic(self.hosts, self.topic)
        self.topic_partitions = {p: TopicPartition(self.topic, p) for p in self.partitions}
        self.consumers = {p: KafkaConsumer(bootstrap_servers=self.hosts) for p in self.partitions}
        map(lambda (p, c): c.assign([self.topic_partitions[p]]), self.consumers.items())
        if 'timestamp_extractor' in config:
            self.timestamp_extractor = config['timestamp_extractor']

    def get_partitions(self):
        return self.partitions

    def get_earliest_offsets(self):
        offsets = {p: _get_partition_earliest_offset(
            self.consumers[p],
            self.topic_partitions[p]) for p in self.partitions}
        logger.info("get earliest offsets %s", offsets)
        return offsets

    def get_latest_offsets(self):
        offsets = {p: _get_partition_latest_offset(
            self.consumers[p],
            self.topic_partitions[p]) for p in self.partitions}
        logger.info("get latest offsets %s", offsets)
        return offsets

    def get_offsets_by_timestamp(self, timestamp):
        offsets = {p: _get_partition_offset_by_timestamp(
            self.consumers[p],
            self.topic_partitions[p],
            timestamp,
            self.timestamp_extractor) for p in self.partitions}
        logger.info("get offsets by timestamp %d, %s", timestamp, offsets)
        return offsets

    def get_timestamp_by_offsets(self, offsets):
        timestamps = {p: _get_partition_timestamp_by_offset(
            self.consumers[p],
            self.topic_partitions[p],
            offsets[p],
            self.timestamp_extractor) for p in self.partitions}
        return timestamps

    def __exit__(self, exc_type, exc_value, traceback):
        map(lambda (p, c): c.stop(), self.consumers.items())

class ZKOffsetManager:
    def __init__(self, config, partitions):
        self.hosts = config['hosts']
        self.prefix = config['prefix']
        self.user = config['user']
        self.topic = config['topic']
        self.partitions = partitions
        self.client = KazooClient(self.hosts)
        self.client.start()
        self._check_path()

    def _get_znode(self):
        path = '{}/{}/{}'.format(self.prefix, self.user, self.topic)
        return path

    def _get_zpath(self, partition):
        path = '{}/{}/{}/{}'.format(self.prefix, self.user, self.topic, partition)
        return path

    def _check_path(self):
        self.client.ensure_path(self._get_znode())
        for p in self.partitions:
            path = self._get_zpath(p)
            if not self.client.exists(path):
                self.client.create(path, '-1')

    def _get_single_offset(self, p):
        offset, znode_stat = self.client.get(self._get_zpath(p))
        return int(offset)

    def get_offsets(self):
        offsets = {p: self._get_single_offset(p) for p in self.partitions}
        return offsets

    def _set_single_offset(self, p, o):
        self.client.set(self._get_zpath(p), str(o))

    def set_offsets(self, offsets):
        for p, o in offsets.iteritems():
            self._set_single_offset(p, o)

    def __exit__(self, exc_type, exc_value, traceback):
        self.client.stop()
