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
kafka_rdd_manager.process(rdd_processer)

sc.stop()
```
