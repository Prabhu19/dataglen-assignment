#!/bin/bash
echo "Enter the version of Spark"
echo "If nothing is given as input it would default to 2.0.0"

read sparkversion

if [ ! -n "$sparkversion" ]; then
        sparkversion="2.0.0"
fi



echo "Enter the version of Scala"
echo "If nothing is given as input it would default to 2.11"

read scalaversion

if [ ! -n "$scalaversion" ]; then
        scalaversion="2.11"
fi

nohup spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_$scalaversion:$sparkversion  sparkStreaming.py &
