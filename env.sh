#!/bin/bash
export KAFKA_REPO=/Users/pablomunoz/repo/kafka
export KAFKA_HOME=/Users/pablomunoz/kafka/kafka_2.11-1.0.0
export PYTHONPATH=${KAFKA_REPO}/lib:$PYTHONPATH
export PATH=${KAFKA_REPO}/bin:$PATH
echo Python Search Path $PYTHONPATH

