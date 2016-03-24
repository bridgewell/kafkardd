KafkaRDD
=======

Installation
-------------

    pip install git+ssh://git@192.168.65.220:10022/shutong/kafkardd.git

or

    pip install git+ssh://git@192.168.65.220:10022/shutong/kafkardd.git@0.0.1

for specific version


Usage
---------------------

```python
# you SHOULD set HADOOP_CONF_DIR in you spark-env.sh
# prepare spark context
import pyspark
sc = pyspark.SparkContext()

def rdd_processer(msg_rdd):
	print msg_rdd.take(10)

from kafkardd.kafkardd import KafkaRDDManager

kafka_rdd_config = {
	'spark_context': sc,
	'chunk_size': 0,
	'parallelism': 0,
	'start_policy': {'type': 'committed'},
	'end_policy': {'type': 'latest'},
	'kafka': {
		'hosts': 'localhost:9092',
		'topic': 'test_topic'
		},
	'zookeeper': {
		'hosts': 'localhost:2181',
		'prefix': '/path/to/keep/offset',
		'user': 'test',
		'topic': 'test_topic'
		}
	}
kafka_rdd_manager = KafkaRDDManager(kafka_rdd_config)
kafka_rdd_manager.process(rdd_processer)

sc.stop()
```
