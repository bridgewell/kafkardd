# -*- coding: utf-8 -*-
import logging

from kafka import KafkaConsumer
from kafka.common import TopicPartition

logger = logging.getLogger(__name__)

def _peek_msg_timestamp_by_offset(consumer, offset, timestamp_extractor):
    topic_partition = list(consumer.assignment())[0]
    consumer.seek(topic_partition, offset)
    msg = consumer.next()
    timestamp = timestamp_extractor(msg.value)
    return timestamp


def _bsearch_offset_by_timestamp(consumer, start_offset, end_offset, timestamp, timestamp_extractor):
    """Search the largest offset that the message timestamp less than target timestamp
    Args:
        consumer (KafkaConsumer): kafka consumer for specific topic partition
        start_offset (int): search start range
        end_offset (int): search end range
        timestamp (int): target timestamp
        timestamp_extractor (callback): callback to return timestamp extracted from a kafka message
    Returns:
        int
    """
    if start_offset == end_offset:
        t = _peek_msg_timestamp_by_offset(consumer, start_offset, timestamp_extractor)
        if t < timestamp:
            return start_offset
        else:
            return start_offset - 1
    peek_offset = (start_offset + end_offset) // 2
    t = _peek_msg_timestamp_by_offset(consumer, peek_offset, timestamp_extractor)
    if t < timestamp:
        start_offset = peek_offset + 1
    else:
        end_offset = peek_offset
    offset = _bsearch_offset_by_timestamp(consumer, start_offset, end_offset, timestamp, timestamp_extractor)
    return offset


def _get_partition_offset_by_timestamp(consumer, timestamp, timestamp_extractor):
    start_offset = _get_partition_earliest_offset(consumer)
    end_offset = _get_partition_latest_offset(consumer)
    offset = _bsearch_offset_by_timestamp(consumer, start_offset, end_offset, timestamp, timestamp_extractor)
    return offset


def _get_partition_earliest_offset(consumer):
    topic_partition = list(consumer.assignment())[0]
    consumer.seek_to_beginning()
    offset = consumer.position(topic_partition)
    return offset


def _get_partition_latest_offset(consumer):
    topic_partition = list(consumer.assignment())[0]
    consumer.seek_to_end()
    offset = consumer.position(topic_partition)
    return offset


def _get_partitions_by_topic(hosts, topic):
    consumer = KafkaConsumer(bootstrap_servers=hosts)
    partitions = list(consumer.partitions_for_topic(topic))
    consumer.close()
    return partitions


class KafkaOffsetManager(object):
    """Kafka offset fetcher
    Args:
        config (dict): configuration
            'hosts' (str) : hosts list of Kafka cluster seperated by ','
                ex. '192.168.67.65:9092,192.168.67.67:9092'
            'topic' (str) : Kafka topic to fetch
            'timestamp_extractor' (callback, optional) : function to extract timestamp from messages
                required for get_offsets_by_timestamp() and get_timestamp_by_offsets()
    """
    def __init__(self, config):
        hosts = config['hosts'].split(',')
        topic = config['topic']
        self.partitions = _get_partitions_by_topic(hosts, topic)
        self._consumers = {}
        for p in self.partitions:
            self._consumers[p] = KafkaConsumer(bootstrap_servers=hosts)
            self._consumers[p].assign([TopicPartition(topic, p)])
        self._timestamp_extractor = config.get('timestamp_extractor')

    def get_earliest_offsets(self):
        """Get earliest offsets of each partitions
        Args:
            None
        Returns:
            dict : offsets of each partitions { pid (int) : offset (int) }
        """
        offsets = {p: _get_partition_earliest_offset(self._consumers[p]) for p in self.partitions}
        logger.info("get earliest offsets %s", offsets)
        return offsets

    def get_latest_offsets(self):
        """Get latest offsets of each partitions
        Args:
            None
        Returns:
            dict : offsets of each partitions { pid (int) : offset (int) }
        """
        offsets = {p: _get_partition_latest_offset(self._consumers[p]) for p in self.partitions}
        logger.info("get latest offsets %s", offsets)
        return offsets

    def get_offsets_by_timestamp(self, timestamp):
        """Get latest offsets of each partitions which message timestamp less than target timestamp
        Assume timestamp of messages in each partitions are monotone increasing
        Args:
            timestamp (int) : target timestamp
        Returns:
            dict : offsets of each partitions { pid (int) : offset (int) }
        """
        offsets = {p: _get_partition_offset_by_timestamp(
            self._consumers[p],
            timestamp,
            self._timestamp_extractor) for p in self.partitions}
        logger.info("get offsets by timestamp %d, %s", timestamp, offsets)
        return offsets

    def get_timestamp_by_offsets(self, offsets):
        """Get timestamp of each partitions with given offsets
        Args:
            offsets (dict) : { pid : offset }
        Returns:
            dict : timestamps of each partitions { pid (int) : offset (int) }
        """
        timestamps = {p: _peek_msg_timestamp_by_offset(
            self._consumers[p],
            offsets[p],
            self._timestamp_extractor) for p in self.partitions}
        return timestamps

    def stop(self):
        """Stop manager
        Args:
            None
        Returns:
            None
        """
        for (p, c) in self._consumers.items():
            c.close()
