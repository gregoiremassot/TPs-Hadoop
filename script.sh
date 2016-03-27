#!/bin/bash
clear
hadoop fs  -mkdir gregoire_massot
hadoop fs -mkdir gregoire_massot/input
hadoop fs -rm gregoire_massot/output/part-r-00000
hadoop fs -rm gregoire_massot/output/_SUCCESS
hadoop fs -rmdir gregoire_massot/output
hadoop fs -copyFromLocal *.txt  gregoire_massot/input
rm part-r-00000

hadoop com.sun.tools.javac.Main WordCount.java
jar cf wc.jar WordCount*.class
hadoop jar wc.jar WordCount gregoire_massot/input gregoire_massot/output
hdfs dfs -copyToLocal gregoire_massot/output/part-r-00000
