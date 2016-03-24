# -*- coding: utf-8 -*-
import logging

from pyspark.streaming.kafka import KafkaUtils, OffsetRange

from offset_manager import ZKOffsetManager, KafkaOffsetManager
from util import generate_chunk, split_chunks_by_parallelism

logger = logging.getLogger(__name__)

class KafkaRDDManager:
    def __init__(self, config):
        self.sc = config['spark_context']
        self.chunk_size = config['chunk_size']
        self.parallelism = config['parallelism']
        self.kafka_hosts = config['kafka']['hosts']
        self.kafka_topic = config['kafka']['topic']
        self.kafka_offset_manager = KafkaOffsetManager(config['kafka'])
        self.partitions = self.kafka_offset_manager.get_partitions()
        self.zk_offset_manager = ZKOffsetManager(config['zookeeper'], self.partitions)

        self.fetch_offset_ranges_by_policy(config['start_policy'], config['end_policy'])

    def _commit_offsets(self, offsets):
        self.zk_offset_manager.set_offsets(offsets)

    def _fetch_offsets_by_policy(self, policy):
        if policy['type'] == 'earliest':
            offsets = self.kafka_offset_manager.get_earliest_offsets()
        elif policy['type'] == 'latest':
            offsets = self.kafka_offset_manager.get_latest_offsets()
        elif policy['type'] == 'timestamp':
            offsets = self.kafka_offset_manager.get_offsets_by_timestamp(policy['timestamp'])
        elif policy['type'] == 'committed':
            offsets = self.zk_offset_manager.get_offsets()
        return offsets

    def fetch_offset_ranges_by_policy(self, start_policy, end_policy):
        start_offsets = self._fetch_offsets_by_policy(start_policy)
        end_offsets = self._fetch_offsets_by_policy(end_policy)
        self.offset_ranges = {p: (start_offsets[p], end_offsets[p]) for p in self.partitions}
        return self.offset_ranges

    def _process_single_chunk(self, chunk, msg_rdd_processor):
        kafka_params = {"metadata.broker.list": self.kafka_hosts}
        split_chunks = split_chunks_by_parallelism(chunk, self.parallelism)
        offset_ranges = [OffsetRange(self.kafka_topic, partition=p, fromOffset=s, untilOffset=e)
                         for (p, s, e) in split_chunks if s < e]
        logger.info("chunk offset ranges:")
        for r in offset_ranges:
            logger.info("%s, %d: %d to %d" % (r.topic, r.partition, r.fromOffset, r.untilOffset))
        # override the default utf8_decoder, let the value be bytes
        keyed_msg_rdd = KafkaUtils.createRDD(self.sc, kafka_params, offset_ranges, valueDecoder=lambda x: x)
        msg_rdd = keyed_msg_rdd.values()
        msg_rdd_processor(msg_rdd)

    def process(self, msg_rdd_processor):
        for chunk in generate_chunk(self.offset_ranges, self.chunk_size):
            self._process_single_chunk(chunk, msg_rdd_processor)
            self._commit_offsets({p: e for (p, s, e) in chunk})
