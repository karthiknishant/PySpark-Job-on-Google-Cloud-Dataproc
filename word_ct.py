from pyspark.sql import SparkSession
import argparse
import sys
def word_count(input_file, output_dir):
    # Create a SparkSession
    spark = SparkSession.builder.appName("WordCount").getOrCreate()

    # Read input text file into an RDD
    lines_rdd = spark.sparkContext.textFile(input_file)

    # Map phase: Split each line into words and create key-value pairs (word, 1)
    words_rdd = lines_rdd.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1))

    # Reduce phase: Sum the counts of each word
    word_counts_rdd = words_rdd.reduceByKey(lambda count1, count2: count1 + count2)

    # Save the word counts to the output directory as text files
    word_counts_rdd.saveAsTextFile(output_dir)

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_file> <output_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    word_count(input_file, output_file)
