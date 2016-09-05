# -*- coding: utf-8 -*-
import logging

from pyspark.streaming.kafka import KafkaUtils, OffsetRange

from kafkaoffset import KafkaOffsetManager
from zkoffset import ZKOffsetManager
from util import generate_chunk, split_chunks_by_parallelism

logger = logging.getLogger(__name__)

class OffsetPolicy:
    """Offset policy for KafkaRDDManager
    Args:
        type (str): policy type, one of 'earliest', 'latest', 'committed', or 'timestamp'
            'earliest' : fetch offssets at the earliest kept by Kafka
            'latest' : fetch offssets at the latest kept by Kafka
            'committed' : fetch offsets at the Zookeeper
            'timestamp' : fetch offsets searched with the target timestamp in the Kafka messages
        timestamp (int, required if type is 'timestamp'): target timestamp to be searched
    """
    def __init__(self, type, timestamp=None):
        if type not in ['earliest', 'latest', 'committed', 'timestamp']:
            raise Exception('Policy type \"%s\" not defined' % type)
        if type == 'timestamp' and timestamp is None:
            raise Exception('Policy type \"timestamp\" require the timestamp parameter')
        self.type = type
        self.timestamp = timestamp


class KafkaRDDManager(object):
    def __init__(self, config):
        self._sc = config['spark_context']
        self._chunk_size = config.get('chunk_size', 0)
        self._parallelism = config.get('parallelism', 0)
        self._kafka_param = {'metadata.broker.list': config['kafka']['hosts']}
        self._kafka_topic = config['kafka']['topic']
        self._kafka_offset_manager = KafkaOffsetManager(config['kafka'])
        self._partitions = self._kafka_offset_manager.partitions
        if 'zookeeper' in config:
            self._zk_offset_manager = ZKOffsetManager(config['zookeeper'], self._partitions)
        else:
            self._zk_offset_manager = None

        self.set_offset_ranges_by_policy(config['start_policy'], config['end_policy'])

    def get_offset_ranges_by_policy(self, start_policy, end_policy):
        """Get offset ranges of each partitions by start and end policy
        Args:
            start_policy (OffsetPolicy): offset ranges starting from
            end_policy (OffsetPolicy): offset ranges ending to
        Returns:
            dict : offset ranges of each partitions { pid (int) : (start (int), end (int)) }
        """
        start_offsets = self._fetch_offsets_by_policy(start_policy)
        end_offsets = self._fetch_offsets_by_policy(end_policy)
        offset_ranges = {p: (start_offsets[p], end_offsets[p]) for p in self._partitions}
        return offset_ranges

    def set_offset_ranges_by_policy(self, start_policy, end_policy):
        """Set offset ranges of each partitions by start and end policy
        Args:
            start_policy (OffsetPolicy): offset ranges starting from
            end_policy (OffsetPolicy): offset ranges ending to
        Returns:
            None
        """
        offset_ranges = self.get_offset_ranges_by_policy(start_policy, end_policy)
        self._offset_ranges = offset_ranges

    def _fetch_offsets_by_policy(self, policy):
        if policy.type == 'earliest':
            offsets = self._kafka_offset_manager.get_earliest_offsets()
        elif policy.type == 'latest':
            offsets = self._kafka_offset_manager.get_latest_offsets()
        elif policy.type == 'timestamp':
            offsets = self._kafka_offset_manager.get_offsets_by_timestamp(policy.timestamp)
        elif policy.type == 'committed':
            offsets = self._zk_offset_manager.get_offsets()
        return offsets

    def process(self, msg_rdd_processor, commit_policy=None):
        """Process the Spark RDD created by spark.streaming.kafka
        Args:
            msg_rdd_processor (callback): callback function to process RDD fetched from kafka
            commit_policy (str): one of 'after', 'before', or None for committing action
        Returns:
            None
        """
        logger.info("working on offset ranges, %s", self._offset_ranges)
        for chunk in generate_chunk(self._offset_ranges, self._chunk_size):
            logger.info("working on chunked offset ranges, %s", chunk)
            offset_ranges = self._compose_chunk_offset_ranges(chunk)
            if commit_policy == 'before':
                self._commit_offsets({p: e for (p, s, e) in chunk})
            self._process_single_chunk(offset_ranges, msg_rdd_processor)
            if commit_policy == 'after':
                self._commit_offsets({p: e for (p, s, e) in chunk})
            logger.info("completing on chunked offset ranges, %s", chunk)

    def fetch(self):
        """Fetch the Spark RDD created by spark.streaming.kafka
        Args:
            None
        Returns:
            Spark RDD data
        """
        logger.info("fetching on offset ranges, %s", self._offset_ranges)
        rdd_list = []
        for chunk in generate_chunk(self._offset_ranges, self._chunk_size):
            logger.info("fetching on chunked offset ranges, %s", chunk)
            offset_ranges = self._compose_chunk_offset_ranges(chunk)
            rdd = self._fetch_single_chunk(offset_ranges)
            rdd_list.append(rdd)
        return self._sc.union(rdd_list)

    def _compose_chunk_offset_ranges(self, chunk):
        split_chunks = split_chunks_by_parallelism(chunk, self._parallelism)
        offset_ranges = [OffsetRange(self._kafka_topic, partition=p, fromOffset=s, untilOffset=e)
                         for (p, s, e) in split_chunks if s < e]
        return offset_ranges

    def _process_single_chunk(self, offset_ranges, msg_rdd_processor):
        keyed_msg_rdd = KafkaUtils.createRDD(self._sc, self._kafka_param, offset_ranges, valueDecoder=lambda x: x)
        msg_rdd = keyed_msg_rdd.values()
        msg_rdd_processor(msg_rdd)

    def _fetch_single_chunk(self, offset_ranges):
        keyed_msg_rdd = KafkaUtils.createRDD(self._sc, self._kafka_param, offset_ranges, valueDecoder=lambda x: x)
        msg_rdd = keyed_msg_rdd.values()
        return msg_rdd

    def _commit_offsets(self, offsets):
        if self._zk_offset_manager is not None:
            self._zk_offset_manager.set_offsets(offsets)

    def stop(self):
        """Stop manager
        Args:
            None
        Returns:
            None
        """
        if self._kafka_offset_manager is not None:
            self._kafka_offset_manager.stop()
        if self._zk_offset_manager is not None:
            self._zk_offset_manager.stop()
