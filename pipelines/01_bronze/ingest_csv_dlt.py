import dlt
from pyspark.sql.functions import current_timestamp, col

source_path = spark.conf.get("pipeline.landing_path")

@dlt.table(
    name="county_metrics_csv_dlt",
    comment="CSV Chunk 4 loaded via Delta Live Tables",
)
def ingest_csv_chunk4():
    """
    Ingests the fourth CSV chunk using Auto Loader within DLT.
    """
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .option("pathGlobFilter", "*chunk_4.csv")
        .load(source_path)
        .withColumn("load_dt", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )