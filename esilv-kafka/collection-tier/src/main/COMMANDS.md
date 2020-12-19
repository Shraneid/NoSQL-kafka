
$ docker exec -it <container_id> bash

$ kafka-topics --create --zookeeper localhost:2181 --topic velib-nbfreedocks-count-notifications --replication-factor 1 --partitions 1 --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01"
$ kafka-consumer-groups -bootstrap-server localhost:9092 -describe -group group-test-1

$ kafka-topics --zookeeper localhost:2181 --list

$ kafka-topics --delete --zookeeper localhost:2181 --topic velib-stats-raw
$ kafka-topics --delete --zookeeper localhost:2181 --topic velib-nbfreedocks-updates
$ kafka-topics --delete --zookeeper localhost:2181 --topic velib-nbfreedocks-count-notifications



docker exec -it 3488cc568c6f bash

kafka-topics –-zookeeper localhost:2181 --create --topic compact --config "cleanup.policy=compact" --config "delete.retention.ms=100" --config "segment.ms=100" --config "min.cleanable.dirty.ratio=0.01"

kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic esilv-sample1
kafka-topics --zookeeper localhost:2181 --delete --topic remove-me

kafka-console-producer --broker-list localhost:9092 --topic esilv-sample1
Hello World ESILV Group

kafka-console-consumer --bootstrap-server localhost:9092 --topic blockcypher --from-beginning
