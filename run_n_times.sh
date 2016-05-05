#!/bin/bash

for i in $(seq 1 $1);
do
  reset;
  sh clearLogs.sh; 
  sh captureLogs.sh; 
  nohup ./simpledynamo-grading.linux SimpleDynamo/app/build/outputs/apk/app-debug.apk; 
  for l in `cat pid.pid`;
  do 
    kill $l;
  done;
done
