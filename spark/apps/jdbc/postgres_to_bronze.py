import argparse
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max
from pyspark.sql.types import StringType, StructField, StructType


def parse_args():
    parser = argparse.ArgumentParser(
        description="Incrementally import a PostgreSQL table to HDFS bronze using Spark JDBC."
    )
    parser.add_argument("--jdbc-url", default="jdbc:postgresql://db-ecom:5432/ecom")
    parser.add_argument("--jdbc-driver", default="org.postgresql.Driver")
    parser.add_argument("--user", default="postgres")
    parser.add_argument("--password", default="postgres")
    parser.add_argument("--schema", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--target-path", required=True)
    parser.add_argument("--incremental-column", default="id")
    parser.add_argument("--state-root", default="/data/bronze/_jdbc_state")
    return parser.parse_args()


def hdfs_uri(path):
    return path if path.startswith("hdfs://") else f"hdfs://namenode:9000{path}"


def read_last_value(spark, state_path):
    try:
        state_df = spark.read.json(hdfs_uri(state_path))
        row = state_df.orderBy(col("updated_at").desc()).select("last_value").first()
        return row["last_value"] if row and row["last_value"] is not None else "0"
    except Exception:
        return "0"


def write_last_value(spark, state_path, last_value):
    schema = StructType(
        [
            StructField("last_value", StringType(), False),
            StructField("updated_at", StringType(), False),
        ]
    )
    updated_at = datetime.now(timezone.utc).isoformat()
    spark.createDataFrame([(str(last_value), updated_at)], schema).coalesce(1).write.mode(
        "overwrite"
    ).json(hdfs_uri(state_path))


def main():
    args = parse_args()
    spark = SparkSession.builder.appName(
        f"PostgresToBronze_{args.schema}_{args.table}"
    ).getOrCreate()

    state_path = f"{args.state_root}/{args.schema}/{args.table}"
    last_value = read_last_value(spark, state_path)
    table_ref = f'"{args.schema}"."{args.table}"'
    query = (
        f"(SELECT *, CURRENT_TIMESTAMP AS load_timestamp "
        f"FROM {table_ref} "
        f"WHERE {args.incremental_column} > {last_value}) AS src"
    )

    df = (
        spark.read.format("jdbc")
        .option("url", args.jdbc_url)
        .option("driver", args.jdbc_driver)
        .option("dbtable", query)
        .option("user", args.user)
        .option("password", args.password)
        .load()
    )

    if df.rdd.isEmpty():
        print(f"No new rows for {args.schema}.{args.table} after {last_value}")
        spark.stop()
        return

    row_count = df.count()
    max_value = df.agg(spark_max(col(args.incremental_column))).collect()[0][0]

    (
        df.write.mode("append")
        .option("header", "false")
        .option("nullValue", "")
        .option("emptyValue", "")
        .csv(hdfs_uri(args.target_path))
    )

    write_last_value(spark, state_path, max_value)
    print(
        f"Imported {row_count} rows from {args.schema}.{args.table} "
        f"to {args.target_path}; last {args.incremental_column}={max_value}"
    )
    spark.stop()


if __name__ == "__main__":
    main()
