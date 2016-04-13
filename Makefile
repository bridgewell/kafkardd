KAFKA_TEST_SERVER=localhost
KAFKA_TEST_PORT=9092
ZK_TEST_SERVER=localhost
ZK_TEST_PORT=2181

test:
	python -m pytest \
		--cov kafkardd \
		--zk_host=${ZK_TEST_SERVER}:${ZK_TEST_PORT} \
		--kafka_host=${KAFKA_TEST_SERVER}:${KAFKA_TEST_PORT}

setup_test:
	python setup.py pytest --addopts '--cov kafkardd'

.PHONY: test
