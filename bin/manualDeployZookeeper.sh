#!/bin/bash
#
# Pablo Munoz (c) 2018
#
# Kafka system deploy
#
KAFKA_REPO=/Users/pablomunoz/repo/kafka
KAFKA_HOME=/Users/pablomunoz/kafka/kafka_2.11-1.0.0

#Launch  zookeeper
echo Launching Zookeeper instance
${KAFKA_HOME}/bin/zookeeper-server-start.sh ${KAFKA_REPO}/etc/zookeeper.properties

