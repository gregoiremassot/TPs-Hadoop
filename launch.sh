#!/bin/bash

# Remove the old output files
hadoop fs -rm -r output

# Compile and launch java program
hadoop com.sun.tools.javac.Main pageRank.java
jar cf pageRank.jar pageRank*.class
hadoop jar pageRank.jar pageRank input output

# Get back output files on local disk
hadoop fs -get output/part-r-00000

