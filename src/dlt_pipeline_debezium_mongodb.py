"""Delta Live Tables pipeline: Debezium (MongoDB) CDC ingestion.

Debezium's MongoDB connector emits the document as a single JSON string
(MongoDB extended JSON) with no schema metadata at all — the classic
schema-parsing gap of schemaless sources. This pipeline closes that gap
with the VARIANT data type instead of schema inference: the document is
parsed once with ``parse_json`` and stored as-is, so schema drift needs
no schema registry, no inference pass, and no pipeline changes.
Consumers project typed columns at read time (``doc:address.city``),
or through the optional typed projection view below.

Bronze: raw Debezium envelopes ingested incrementally with Auto Loader
        (the envelope schema is fixed for every collection, because the
        document payload is just a string).
Silver: current state of the collection, one row per ``_id``, the
        document as a VARIANT column, deletes applied.

Pipeline configuration:
    pipeline.landing_root (required)  directory the Debezium events land
                                      in, e.g. a Unity Catalog volume
                                      "/Volumes/<catalog>/landing/debezium"
    pipeline.database     (required)  source MongoDB database name
    pipeline.collection   (required)  source MongoDB collection name
    pipeline.topic_prefix (optional)  Debezium topic prefix, defaults to
                                      "mongodb"
    pipeline.topic        (optional)  landing subdirectory; defaults to
                                      "<topic_prefix>.<database>.<collection>"
    pipeline.projection   (optional)  semicolon-separated SQL expressions
                                      over the ``doc`` VARIANT column; if
                                      set, a "<collection>_typed"
                                      materialized view is created, e.g.
                                      "doc:name::string AS name;
                                       doc:address.city::string AS city"

Connector requirements:
    - capture.mode=change_streams_update_full (updates carry the full
      document, not an oplog patch)
    - change_streams_with_pre_images if deletes must be applied: delete
      events then carry the document in ``before``. Without pre-images a
      delete has no document and no ``_id``; such events are dropped and
      counted by the valid_document_id expectation.
"""

import dlt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

spark = SparkSession.getActiveSession()


def _required_setting(key: str) -> str:
    value = spark.conf.get(key, None)
    if not value:
        raise ValueError(
            f"Pipeline configuration '{key}' must be set (see the module "
            "docstring for all settings)."
        )
    return value


landing_root = _required_setting("pipeline.landing_root").rstrip("/")
database = _required_setting("pipeline.database")
collection = _required_setting("pipeline.collection")
topic_prefix = spark.conf.get("pipeline.topic_prefix", "mongodb")
topic = spark.conf.get("pipeline.topic", None) or (
    f"{topic_prefix}.{database}.{collection}"
)
projection = spark.conf.get("pipeline.projection", None)

RAW_PATH = f"{landing_root}/{topic}"

# The Debezium MongoDB envelope is identical for every collection: the
# document travels as a string, so unlike relational connectors there is
# no per-table schema to derive. `ord` is the oplog ordinal within a
# cluster-time second — the tiebreaker for same-millisecond events.
SOURCE_SCHEMA = StructType(
    [
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("rs", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("ord", LongType(), True),
    ]
)

ENVELOPE = StructType(
    [
        StructField(
            "payload",
            StructType(
                [
                    StructField("before", StringType(), True),
                    StructField("after", StringType(), True),
                    StructField("source", SOURCE_SCHEMA, True),
                    StructField("op", StringType(), True),
                    StructField("ts_ms", LongType(), True),
                ]
            ),
            True,
        ),
        StructField("schema", StringType(), True),
    ]
)

# MongoDB's _id in extended JSON is usually {"$oid": "..."} but may be
# any scalar or even a composite document. The merge key must be a
# stable string, so try each representation in turn.
DOC_ID_EXPR = (
    "coalesce("
    "try_variant_get(doc, '$._id[\"$oid\"]', 'string'), "
    "try_variant_get(doc, '$._id', 'string'), "
    "to_json(try_variant_get(doc, '$._id'))"
    ")"
)


@dlt.table(
    name=f"bronze_{collection}",
    comment=f"Raw Debezium CDC events for {database}.{collection}, "
    "incrementally ingested with Auto Loader",
    table_properties={"quality": "bronze"},
)
def bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .schema(ENVELOPE)
        .load(RAW_PATH)
    )


@dlt.view(
    name=f"bronze_clean_{collection}",
    comment=f"Parsed CDC feed for {collection} (drives apply_changes).",
)
@dlt.expect_or_drop("valid_op", "_cdc_op IS NOT NULL")
@dlt.expect_or_drop("valid_document_id", "_id IS NOT NULL")
def bronze_clean():
    events = dlt.read_stream(f"bronze_{collection}").select("payload.*")
    # Deletes carry the document in `before` (pre-images); everything
    # else carries the full document in `after`.
    parsed = events.select(
        F.col("op").alias("_cdc_op"),
        F.col("ts_ms").alias("_cdc_ts_ms"),
        F.col("source.ord").alias("_cdc_ord"),
        F.col("source.db").alias("_cdc_source_db"),
        F.col("source.collection").alias("_cdc_source_collection"),
        F.expr(
            "parse_json(CASE WHEN op = 'd' THEN before ELSE after END)"
        ).alias("doc"),
    )
    return parsed.withColumn("_id", F.expr(DOC_ID_EXPR))


dlt.create_streaming_table(
    name=f"{collection}_final",
    comment=f"Current state of {database}.{collection}: one row per "
    "_id, document stored as VARIANT (deletes applied).",
    table_properties={"quality": "silver"},
)

dlt.apply_changes(
    target=f"{collection}_final",
    source=f"bronze_clean_{collection}",
    keys=["_id"],
    sequence_by=F.struct(F.col("_cdc_ts_ms"), F.col("_cdc_ord")),
    apply_as_deletes=F.expr("_cdc_op = 'd'"),
    except_column_list=[
        "_cdc_op",
        "_cdc_ts_ms",
        "_cdc_ord",
        "_cdc_source_db",
        "_cdc_source_collection",
    ],
)


if projection:
    # Declarative typed projection over the VARIANT document — the
    # schemaless silver table stays untouched; consumers who want
    # columns get them here, and changing the projection is a config
    # edit plus refresh, never a reload.
    projection_exprs = [
        expr.strip() for expr in projection.split(";") if expr.strip()
    ]

    @dlt.table(
        name=f"{collection}_typed",
        comment=f"Typed projection of {collection}_final, defined by "
        "the pipeline.projection setting.",
        table_properties={"quality": "gold"},
    )
    def typed_projection():
        return dlt.read(f"{collection}_final").selectExpr(
            "_id", *projection_exprs
        )
