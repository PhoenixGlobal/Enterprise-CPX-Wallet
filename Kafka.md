#Kafka Cluster Deployment Method
###Step 1.Configure(at each node)
>/config/server.properties</br>
>broker.id=0 (unique)</br>
>log.retention.hours=168</br>
>log.retention.bytes=4294967296</br>

###Step 2.Start(at each node)
>/bin//kafka-server-start.sh config/server.properties
>

###More Command
>1.Create a topic</br>
>	bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test</br>
>2.Send some messages</br>
>	bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test.</br>
>3.Start a consumer</br>
>	bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning.</br>
