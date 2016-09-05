# -*- coding: utf-8 -*-
from pb2df.convert import Converter

from pb_class import PB_CLASS
from kafkardd import KafkaRDDManager

def fetch_kafkadf(kafka_rdd_config,
                  sqlc,
                  pbobj_timestamp_extractor=lambda pb: pb.TimeStamp):
    sc = kafka_rdd_config['spark_context']
    topic = kafka_rdd_config['kafka']['topic']
    if 'timestamp_extractor' not in kafka_rdd_config['kafka']:
        def extractor(raw):
            return pbobj_timestamp_extractor(PB_CLASS[topic].FromString(raw))
        kafka_rdd_config['kafka']['timestamp_extractor'] = extractor
    print kafka_rdd_config
    kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
    rdd = kafka_rdd_manager.fetch()
    proto_rdd = rdd.map(PB_CLASS[topic].FromString)
    df = Converter(PB_CLASS[topic], sc, sqlc).to_dataframe(proto_rdd)
    return df

def process_kafkadf(kafka_rdd_config,
                    sqlc,
                    df_processor,
                    commit_policy=None,
                    pbobj_timestamp_extractor=lambda pb: pb.TimeStamp):
    sc = kafka_rdd_config['spark_context']
    topic = kafka_rdd_config['kafka']['topic']
    if 'timestamp_extractor' not in kafka_rdd_config['kafka']:
        def extractor(raw):
            return pbobj_timestamp_extractor(PB_CLASS[topic].FromString(raw))
        kafka_rdd_config['kafka']['timestamp_extractor'] = extractor
    kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
    decode_protobuf = PB_CLASS[topic].FromString
    to_dataframe = Converter(PB_CLASS[topic], sc, sqlc).to_dataframe

    def rdd_processor(rdd):
        proto_rdd = rdd.map(decode_protobuf)
        df = to_dataframe(proto_rdd)
        df_processor(df)
    kafka_rdd_manager.process(rdd_processor, commit_policy)
