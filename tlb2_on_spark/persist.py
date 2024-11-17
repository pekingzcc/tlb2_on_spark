from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W
from pyspark.sql.dataframe import DataFrame as SparkDF

def persist_spark_df(df, folder=None):
    """
    Persist a Spark DataFrame to disk and read it back.

    Parameters:
    df (pyspark.sql.dataframe.DataFrame): The Spark DataFrame to persist.
    folder (str, optional): The folder path where the DataFrame will be saved.
                            If None, a temporary folder will be used.

    Returns:
    pyspark.sql.dataframe.DataFrame: The reloaded Spark DataFrame.
    """
    import uuid
    if folder is None:
        folder = f"/tmp/persist_df/{uuid.uuid4()}"
    df.write.mode("overwrite").parquet(folder)
    return df.sparkSession.read.parquet(folder)

# Add the persist_spark_df method to the Spark DataFrame class
setattr(SparkDF, "forcePersist", persist_spark_df)