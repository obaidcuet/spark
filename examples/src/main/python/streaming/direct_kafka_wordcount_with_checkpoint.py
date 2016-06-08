#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
 Counts words in UTF8 encoded, '\n' delimited text read from kafka topic. It also confirms at least once using checkpoint. 
 Usage: direct_kafka_wordcount_with_checkpoint.py <broker_list> <topic> <checkpointDir>
 
 Run the example
    `$ bin/spark-submit --jars <if needed> \
	  --driver-library-path <if needed> \
      examples/src/main/python/streaming/direct_kafka_wordcount_with_checkpoint.py \
      brokethost1:9092,brokerhost2:9092 topic_name \
	  hdfs://<HDFS location>/checkpointDirectory`
"""

from __future__ import print_function

import sys
from time import sleep

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def createContext(brokers, topic, checkpointDir):
    # If you do not see this printed, that means the StreamingContext has been loaded
    # from the new checkpoint

    sc = SparkContext(appName="PythonStreamingRecoverableNetworkWordCount")
    ssc = StreamingContext(sc, 1)

    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    wordCounts = lines.flatMap(lambda line: line.split(" ")) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a+b)

    #wordCounts.foreachRDD(echo)
    wordCounts.pprint()
    ssc.checkpoint(checkpointDir)
    return ssc

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: direct_kafka_wordcount_checkpoint.py <broker_list> <topic> <checkpointDir>", file=sys.stderr)
        exit(-1)

    brokers, topic, checkpointDir = sys.argv[1:]

    ssc = StreamingContext.getOrCreate(checkpointDir,
                                       lambda: createContext(brokers, topic, checkpointDir))
    ssc.start()
    ssc.awaitTermination()

