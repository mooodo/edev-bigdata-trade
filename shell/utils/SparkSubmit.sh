#!/bin/bash
if [ $# -lt 2 ]; then
  echo  "The usage is :  SparkSubmit.sh classname jarfilename param1 ..." 
else
  spark-submit --master spark://hadoop-master:7077 --conf spark.debug.maxToStringFields=1000 --class $@
fi
