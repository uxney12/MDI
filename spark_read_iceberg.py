from pyspark.sql import SparkSession

# =====================================================
# 1️⃣ Tạo SparkSession với cấu hình Iceberg + MinIO
# =====================================================
spark = (
    SparkSession.builder
    .appName("Read Iceberg from MinIO")
    # Cấu hình catalog để Spark biết cách đọc Iceberg table
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")  # đường dẫn gốc
    # Cấu hình MinIO (S3 endpoint)
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.catalogImplementation", "hive")
    .getOrCreate()
)

# =====================================================
# 2️⃣ Đọc dữ liệu Parquet trực tiếp từ MinIO
# =====================================================

# Đường dẫn dữ liệu thật trong MinIO (bucket warehouse/MDI/mcc/data)
# Bạn có thể điều chỉnh nếu cấu trúc thực tế khác.
data_path = "s3a://warehouse/MDI/mcc/data"

print("\n=== 🔍 Đang đọc dữ liệu từ:", data_path, "===\n")

# Đọc các file parquet
df = spark.read.parquet(data_path)

# Hiển thị schema và một vài dòng dữ liệu
print("=== ✅ Schema của dữ liệu ===")
df.printSchema()

print("=== ✅ Một vài dòng dữ liệu mẫu ===")
df.show(20, truncate=False)

# =====================================================
# 3️⃣ (Tuỳ chọn) Lưu kết quả transform ra Iceberg table mới
# =====================================================
# Ví dụ: lưu lại vào warehouse/MDI/mcc/output_table
# df.writeTo("my_catalog.mdi.output_table").createOrReplace()

# Dừng SparkSession
spark.stop()
