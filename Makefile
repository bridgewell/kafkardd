#KAFKA_TEST_ID=$(shell date +%s%N | md5sum | head -c16)
KAFKA_TEST_ID=test
KAFKA_HOME=~/Tool/kafka_2.10-0.8.2.1
KAFKA_TEST_SERVER=$(shell ip route get 8.8.8.8 | head -1 | cut -d' ' -f8)
KAFKA_TEST_TOPIC='test_topic'
KAFKA_TEST_PARTITION=3

test: test-setup test-run test-teardown

test-setup:
	@echo '[KAFKA_TEST_${KAFKA_TEST_ID}] Start'
	@docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=${KAFKA_TEST_SERVER} --env ADVERTISED_PORT=9092 --name kafka_${KAFKA_TEST_ID} spotify/kafka
	@sleep 3
	@echo '[KAFKA_TEST_${KAFKA_TEST_ID}] Server "kafka_${KAFKA_TEST_ID}" run at ${KAFKA_TEST_SERVER}:9092'
	@${KAFKA_HOME}/bin/kafka-topics.sh --create --zookeeper ${KAFKA_TEST_SERVER}:2181 --replication-factor 1 --partitions ${KAFKA_TEST_PARTITION} --topic ${KAFKA_TEST_TOPIC}

test-run:
	py.test --kafka_host=${KAFKA_TEST_SERVER}:9092 --kafka_topic=${KAFKA_TEST_TOPIC} --kafka_partition=${KAFKA_TEST_PARTITION}

test-teardown:
	@docker kill kafka_${KAFKA_TEST_ID}
	@echo '[KAFKA_TEST_${KAFKA_TEST_ID}] Server "kafka_${KAFKA_TEST_ID}" stop'
	@docker rm kafka_${KAFKA_TEST_ID}
	@echo '[KAFKA_TEST_${KAFKA_TEST_ID}] End'


.PHONY: test
