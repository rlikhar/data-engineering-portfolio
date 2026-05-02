#!/usr/bin/env bash
set -e

kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic txn-raw --partitions 3 --replication-factor 1
kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic alerts --partitions 3 --replication-factor 1