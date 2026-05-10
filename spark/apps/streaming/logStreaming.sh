spark-submit --master spark://spark-master:7077 \
    --conf spark.cores.max=4 \
    --conf spark.executor.cores=2 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
    /opt/spark-apps/streaming/logStreaming.py &> /opt/spark/logs/logStreaming.log
