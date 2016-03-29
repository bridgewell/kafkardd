SPARK_HOME=/home/weasellin/Develop/spark/spark-1.5.2-bin-hadoop2.6
KAFKA_HOME=~/Tool/kafka_2.10-0.8.2.1
#KAFKA_TEST_SERVER=$(shell ip route get 8.8.8.8 | head -1 | cut -d' ' -f8)
KAFKA_TEST_SERVER=127.0.0.1
KAFKA_TEST_PORT=9092
ZK_TEST_PORT=2181

test:
	export SPARK_HOME=${SPARK_HOME} ; \
	python -m pytest \
		--cov kafkardd \
		--cov-report term-missing \
		--zk_host=${KAFKA_TEST_SERVER}:${ZK_TEST_PORT} \
		--kafka_host=${KAFKA_TEST_SERVER}:${KAFKA_TEST_PORT} \
		--kafka_tool=${KAFKA_HOME}/bin/kafka-topics.sh

.PHONY: test
