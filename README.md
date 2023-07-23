Start Zookeeper -> zookeeper-server-start.bat C:\kafka_2.13-3.5.0\config\zookeeper.properties
Start Kafka ->  kafka-server-start.bat C:\kafka_2.13-3.5.0\config\server.properties
Create topic -> kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic testtopic
List The Topics -> kafka-topics.bat --list --bootstrap-server localhost:9092
To start producer -> kafka-console-producer.bat --broker-list localhost:9092 --topic testtopic
To Start Consumer -> kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic testtopic --from-beginning

Start the consumer:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 kafka_consumer.py

Start the producer:
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 kafka_producer.py

http://localhost:4050/jobs/ -> consumer Spark UI
http://localhost:4060/jobs/ -> Producer Spark UI