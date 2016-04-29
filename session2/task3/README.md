flume install
-------------

`curl -O http://www-eu.apache.org/dist/flume/1.6.0/apache-flume-1.6.0-bin.tar.gz`


`tar xzvf apache-flume-1.6.0-bin.tar.gz -C /opt`


`export PATH=/opt/apache-flume-1.6.0-bin/bin:$PATH`


run kafka
---------

`/usr/hdp/current/kafka-broker/bin/kafka start`


or


`/usr/hdp/2.4.0.0-169/kafka/bin/kafka-server-start.sh /usr/hdp/2.4.0.0-169/kafka/conf/server.properties &`


or UI


create topic
------------

`/usr/hdp/2.4.0.0-169/kafka/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`

and populate it with data

`/usr/hdp/2.4.0.0-169/kafka/bin/kafka-console-producer.sh --broker-list 10.0.2.15:9092 --topic test --new-producer < ~/small.txt`


build project
-------------

`mvn clean install -U`


run it
------

`echo flume_hdfs.sources.src1.interceptors = i1 >> ~/conf.conf`


`echo flume_hdfs.sources.src1.interceptors.i1.type = com.dstepanova.session2.task3.Builder >> ~/conf.conf`


`echo flume_hdfs.sources.src1.interceptors.i1.tagsPath = hdfs:////tmp/hw/tags.txt >> ~/conf.conf`


`cp ~/task3.jar /opt/apache-flume-1.6.0-bin/lib/`


`/usr/bin/flume-ng agent --conf conf --conf-file ~/conf.conf --name flume_hdfs`


