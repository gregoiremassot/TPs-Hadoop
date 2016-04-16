#!/bin/bash

# Initialize hadoop and load the input files

export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop fs -mkdir input
hadoop fs -put tiny_graph.txt input