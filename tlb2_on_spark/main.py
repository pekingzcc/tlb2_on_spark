from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from read_file import read_moneta_file
from read_file import agg_partition
from read_file import process_by_tlb

# Define the path to the input data files, using a wildcard pattern for partitions
path = "gs://conviva-prod-moneta-bucket/iad7/2024/11/11/{03,04,05}/*/ot_{$partitions}_*.avro"
# Replace the placeholder with a comma-separated list of partition numbers
path = path.replace("$partitions", ",".join([str(i) for i in range(0, 101)]))
# Define a checksum value for data validation
ck = '97d7e6d4a54a6f1c0b7d851279795e1417914c5f'
# Print the path and checksum (for debugging purposes)
path, ck

def main():
    """
    Main function to create a Spark session, read data, process it, and stop the Spark session.
    """
    # Create a Spark session
    spark = SparkSession.builder.appName("tlb2_on_spark").getOrCreate()

    # Read the raw data from the specified path using the provided checksum
    df_raw = read_moneta_file(spark, path, ck)

    # Aggregate the raw data by partition
    df_agg = agg_partition(df_raw)

    # Process the aggregated data by TLB (Translation Lookaside Buffer)
    df_tlb = process_by_tlb(df_agg)

    # Trigger an action to count the number of rows in the processed DataFrame
    df_tlb.count()

    # Print a success message
    print("Spark session created successfully!")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()