KafkaRDD
=======

Installation
-------------

    pip install git+http://git@192.168.65.220:10080/shutong/kafkardd.git

or

    pip install git+http://git@192.168.65.220:10080/shutong/kafkardd.git@0.0.6

for specific version


Usage
---------------------

For Kafka message to Spark RDD:

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

For Kafka message to Spark SQL Dataframe:

```python
# you SHOULD set HADOOP_CONF_DIR in you spark-env.sh
from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext()
sqlc = SQLContext(sc)

from kafkardd.kafkadf import fetch_kafkadf
from kafkardd.kafkardd import OffsetPolicy
from time import time

config = { 
    'spark_context': sc, 
    'start_policy': OffsetPolicy('timestamp', timestamp=(time()-100)*1e6),
    'end_policy': OffsetPolicy('latest'),
    'kafka': {
        'hosts': '10.2.5.80:9092,10.2.5.81:9092',
        'topic': 'ECommerce.Buy'
        }   
    }

df = fetch_kafkadf(config, sqlc)
df.show()

sc.stop()
```

Test
-------------

    python setup.py pytest
