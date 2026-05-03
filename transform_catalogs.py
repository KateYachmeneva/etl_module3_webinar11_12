import sys
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType


input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("catalogs-offers-json-transform").getOrCreate()

# 1. Читаем исходный JSON как текст
text_df = spark.read.text(input_path)

json_text = "\n".join(row["value"] for row in text_df.collect())

data = json.loads(json_text)

rows = []

# 2. Разбираем catalogs
for catalog in data.get("catalogs", []):
    catalog_id = str(catalog.get("id", ""))

    rows.append(
        (
            "catalog",
            catalog_id,
            "",
            "",
            "",
            "",
            str(catalog.get("date_start", "")),
            str(catalog.get("date_end", "")),
            str(catalog.get("conditions", "")),
            json.dumps(catalog, ensure_ascii=False),
        )
    )

    # Дополнительно раскрываем связи catalog -> offer
    for offer_id in catalog.get("offers", []):
        rows.append(
            (
                "catalog_offer_link",
                catalog_id,
                str(offer_id),
                "",
                "",
                "",
                str(catalog.get("date_start", "")),
                str(catalog.get("date_end", "")),
                str(catalog.get("conditions", "")),
                json.dumps(
                    {
                        "catalog_id": catalog_id,
                        "offer_id": str(offer_id),
                    },
                    ensure_ascii=False,
                ),
            )
        )

# 3. Разбираем offers
for offer in data.get("offers", []):
    offer_id = str(offer.get("offer_id") or offer.get("id") or "")

    old_price = offer.get("old_price")
    new_price = offer.get("new_price")

    discount_percent = ""

    try:
        if old_price is not None and new_price is not None and float(old_price) != 0:
            discount_percent = str(
                round((float(old_price) - float(new_price)) / float(old_price) * 100, 2)
            )
    except Exception:
        discount_percent = ""

    rows.append(
        (
            "offer",
            "",
            offer_id,
            str(offer.get("name", "")),
            str(offer.get("category", "")),
            discount_percent,
            "",
            "",
            "",
            json.dumps(offer, ensure_ascii=False),
        )
    )

# 4. Задаём явную схему, чтобы Spark не падал на типах
schema = StructType(
    [
        StructField("entity_type", StringType(), True),
        StructField("catalog_id", StringType(), True),
        StructField("offer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("discount_percent", StringType(), True),
        StructField("date_start", StringType(), True),
        StructField("date_end", StringType(), True),
        StructField("conditions", StringType(), True),
        StructField("payload", StringType(), True),
    ]
)

result_df = spark.createDataFrame(rows, schema=schema)

print("TRANSFORMED RESULT:")
result_df.show(truncate=False)

print("TRANSFORMED SCHEMA:")
result_df.printSchema()

# 5. Пишем результат в Object Storage
result_df.coalesce(1).write.mode("overwrite").json(output_path)

spark.stop()