#!/bin/bash

$SPARK_HOME/bin/spark-submit \
    --master spark://kowalskyyy:7077 \
    --packages io.delta:delta-core_2.12:2.0.0 \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
    --conf "spark.executor.memory=4g" \
    main.py