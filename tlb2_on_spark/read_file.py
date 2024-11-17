from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window as W
import pandas as pd

def read_moneta_file(spark, path, ck):

    def decode(b):
        return b.decode("utf-8")

    def decode_gt(gt):
        integer = int.from_bytes(gt, byteorder='big')
        return integer

    def decode_clid(clid):
        (a, b, c, d) = [clid[:4], clid[4:8], clid[8:12], clid[12:]]
        a = int.from_bytes(a, byteorder='big')
        b = int.from_bytes(b, byteorder='big')
        c = int.from_bytes(c, byteorder='big')
        d = int.from_bytes(d, byteorder='big')
        return f"{a}.{b}.{c}.{d}"

    def partition_id(file_name):
        import re
        return re.search(r"(.*)(ot_(\d+).*avro)", file_name).groups()[-1]

    df = spark.read.format("avro").load(path).withColumn(
        "file_name",
        F.input_file_name()
    ).withColumn(
        "ck",
        F.udf(decode)(F.col("headers").getItem(1).getField("value").getField("member6"))
    ).withColumn(
        "gt",
        F.udf(decode_gt)(F.col("headers").getItem(4).getField("value").getField("member6"))
    ).withColumn(
        "clid",
        F.udf(decode_clid)(F.col("headers").getItem(2).getField("value").getField("member6"))
    ).withColumn(
        "tsv",
        F.udf(decode)("value")
    ).withColumn(
        "partition_id",
        F.udf(partition_id)("file_name")
    ).drop("headers", "key")

    df = df.filter(f"ck = '{ck}'").forcePersist("/tmp/persist_df/read_moneta_file")
    return df


def agg_partition(df):
    fields = [
        T.StructField('partition_id', T.StringType()),
        T.StructField('buffer', T.BinaryType())
    ]

    @F.pandas_udf(T.StructType(fields), F.PandasUDFType.GROUPED_MAP)
    def extract_feature(key, pdf: pd.DataFrame):
        pdf = pdf.sort_values(by=['gt'])
        buffer = b""

        for v in pdf["value"]:
            buffer += v + "\n".encode("utf-8")

        data = {
            "partition_id": [str(key[0])],
            "buffer": buffer
        }
        dd = pd.DataFrame(data)
        return dd

    df_agg = df.groupBy("partition_id").apply(extract_feature).forcePersist("/tmp/persist_df/agg_partition")
    return df_agg


def process_by_tlb(df):

    fields = [
        T.StructField('eventName', T.StringType()),
        T.StructField('durationSinceLastEvent', T.LongType()),
        T.StructField('eventTags', T.MapType(keyType=T.StringType(), valueType=T.StringType())),
        T.StructField('origEventName', T.StringType()),
        T.StructField('clientId', T.StringType()),
        T.StructField('sessionStartTimeMs', T.LongType()),
        T.StructField('sessionDuration', T.LongType()),
        T.StructField('pageScreenId', T.StringType()),
        T.StructField('globalEventIndex', T.LongType()),
        T.StructField('time', T.LongType()),
    ]

    @F.pandas_udf(T.StructType(fields), F.PandasUDFType.GROUPED_MAP)
    def run_tlb(key, pdf: pd.DataFrame):
        from pytimeline.timeline_request_config.context import TimelineRequestContext
        import warnings
        warnings.filterwarnings('ignore')

        buffer = pdf["buffer"][0]
        input_file = f"/tmp/{key[0]}.tsv"
        with open(input_file, "wb") as fd:
            fd.write(buffer)

        ctx: TimelineRequestContext = build_dag_with_input(input_file)

        output_file = f"/tmp/{key[0]}.json"

        ctx.dag.outputEvents.store(output_file)
        ctx.dag.clock.store("/tmp/clock.json")

        ctx.set_config(stream={"watermark": {"base-out-of-orderness": "1000s"}})

        rows = ctx.run_batch(output_paths=[output_file], enable_logger=False, stats_at_end=False)

        os.unlink(input_file)
        os.unlink(output_file)

        data = {
            "eventName": [],
            "durationSinceLastEvent": [],
            "eventTags": [],
            "origEventName": [],
            "clientId": [],
            "sessionStartTimeMs": [],
            "sessionDuration": [],
            "pageScreenId": [],
            "globalEventIndex": [],
            "time": []
        }
        for row in rows:
            for k in data.keys():
                data[k].append(row.get(k))

        return pd.DataFrame(data)

    df = df.groupBy("partition_id").apply(run_tlb).forcePersist("/tmp/persist_df/process_by_tlb")
    return df

