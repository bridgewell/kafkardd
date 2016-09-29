KafkaRDD
=======

Installation
-------------

    pip install git+https://github.com/bridgewell/kafkardd.git

Usage
---------------------

For Kafka message to Spark RDD run with spark-submit:

```python
# you SHOULD set HADOOP_CONF_DIR in you spark-env.sh
# prepare spark context
import pyspark
sc = pyspark.SparkContext()

def test_rdd_processer(msg_rdd):
	print msg_rdd.take(10)

from kafkardd.kafkardd import KafkaRDDManager, OffsetPolicy

kafka_rdd_config = {
	'spark_context': sc,
	'start_policy': OffsetPolicy('committed'),
	'end_policy': OffsetPolicy('latest'),
	'kafka': {
		'hosts': 'localhost:9092',
		'topic': 'test_topic'
		},
	'zookeeper': {
		'hosts': 'localhost:2181',
		'znode': '/path/to/keep/offset'
		}
	}
kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
kafka_rdd_manager.process(test_rdd_processer, commit_policy='after')

sc.stop()
```

Test
-------------

    python setup.py pytest

or

    python setup.py pytest --addopts '--cov kafkardd'

for coverage details
