#!/usr/bin/env bash

# Running the spark job
spark-submit --master spark://ip-10-0-0-14.ec2.internal:7077 ~/Migration-Climate/src/spark_batch.py