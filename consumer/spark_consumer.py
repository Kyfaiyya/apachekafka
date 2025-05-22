from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StringType, IntegerType

# Inisialisasi Spark Session
spark = SparkSession.builder \
    .appName("GudangMonitoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Schema untuk suhu dan kelembaban
suhu_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("suhu", IntegerType())

kelembaban_schema = StructType() \
    .add("gudang_id", StringType()) \
    .add("kelembaban", IntegerType())

# Stream dari Kafka untuk suhu
suhu_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load()

suhu_json = suhu_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), suhu_schema).alias("data")) \
    .select("data.*")

# Stream dari Kafka untuk kelembaban
kelembaban_df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load()

kelembaban_json = kelembaban_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), kelembaban_schema).alias("data")) \
    .select("data.*")

# Gabungkan stream suhu dan kelembaban berdasarkan gudang_id dengan inner join
gabungan = suhu_json.join(kelembaban_json, "gudang_id", "inner")

# Filter hanya data yang lengkap (suhu dan kelembaban tidak null)
gabungan_filtered = gabungan.filter(
    col("suhu").isNotNull() & col("kelembaban").isNotNull()
)

# Tambahkan kolom status berdasarkan logika suhu dan kelembaban
peringatan = gabungan_filtered.withColumn(
    "status",
    expr("""
        CASE
            WHEN suhu > 80 AND kelembaban > 70 THEN 'Bahaya tinggi! Barang berisiko rusak'
            WHEN suhu > 80 THEN 'Suhu tinggi, kelembaban normal'
            WHEN kelembaban > 70 THEN 'Kelembaban tinggi, suhu aman'
            ELSE 'Aman'
        END
    """)
)

# Fungsi untuk mencetak output ke console biasa
def format_and_display(batch_df, epoch_id):
    rows = batch_df.collect()
    if not rows:
        return

    print(f"\n--- Batch {epoch_id} ---")
    for row in rows:
        gudang = row["gudang_id"]
        suhu = row["suhu"]
        kelembaban = row["kelembaban"]
        status = row["status"]

        if "Bahaya tinggi" in status:
            prefix = "[PERINGATAN KRITIS]"
        elif "Suhu tinggi" in status:
            prefix = "[Peringatan Suhu Tinggi]"
        elif "Kelembaban tinggi" in status:
            prefix = "[Peringatan Kelembaban Tinggi]"
        else:
            prefix = "[Aman]"

        print(f"{prefix}\nGudang {gudang}:\n- Suhu: {suhu}°C\n- Kelembaban: {kelembaban}%\n- Status: {status}\n")

# Jalankan streaming query dengan foreachBatch menggunakan fungsi di atas
query = peringatan.writeStream \
    .outputMode("append") \
    .foreachBatch(format_and_display) \
    .start()

query.awaitTermination()
