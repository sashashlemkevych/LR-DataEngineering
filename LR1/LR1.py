from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, udf
from pyspark.sql.types import StringType
import os, sys

def process_commit_message(commit_message):
    words = commit_message.split(" ")[:5]
    combinations = [words[i:i + 3] for i in range(3)]
    return [" ".join(combination) for combination in combinations]

generate_text_udf = udf(lambda commit: process_commit_message(commit), StringType())

def initialize_spark_session():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    return SparkSession.builder.appName("3GRAMS").getOrCreate()

def read_json_file(file_path):
    return spark.read.json(file_path)

def process_commits(df):
    return (
        df.filter(col('type') == 'PushEvent')
          .select(explode('payload.commits').alias('commit'))
          .select('commit.author.name', 'commit.message')
    )

def add_3_grams_column(df):
    return df.withColumn('3_grams', generate_text_udf(col('message'))).drop('message')

def main():
    global spark
    spark = initialize_spark_session()

    # Read the JSON file
    df_git = read_json_file("10K.github.jsonl")

    # Process commits
    df_author_message = process_commits(df_git)

    # Add 3_grams column
    result_df = add_3_grams_column(df_author_message)

    # Print schema and show first few rows
    result_df.printSchema()
    result_df.show(truncate=False)

    # Save results to a CSV file
    result_df.toPandas().to_csv("output.csv", index=False)


    # Close SparkSession
    spark.stop()

if __name__ == '__main__':
    main()