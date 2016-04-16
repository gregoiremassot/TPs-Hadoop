#!/bin/bash

# Initialize hadoop and load the input files
JAVA_HOME=/usr/java/jdk1.7.0_67-cloudera/
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop fs -mkdir input
hadoop fs -put tiny_graph.txt input