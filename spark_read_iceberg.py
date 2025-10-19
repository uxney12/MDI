from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys
sys.stdout.reconfigure(encoding='utf-8')

# === Tạo SparkSession ===
spark = (
    SparkSession.builder
    .appName("Normalize MCC + Mpass Data")
    .config("spark.sql.catalog.my_catalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.my_catalog.type", "hadoop")
    .config("spark.sql.catalog.my_catalog.warehouse", "s3a://warehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

# === Đường dẫn nguồn ===
mcc_path = "s3a://warehouse/MDI/mcc/data/mcc.parquet"
mpass_path = "s3a://warehouse/Mpass/mpass/data/mpass.parquet"

print(f"=== Đang đọc dữ liệu MCC từ: {mcc_path} ===")
df_mcc = spark.read.parquet(mcc_path)
print(f"=== Đang đọc dữ liệu Mpass từ: {mpass_path} ===")
df_mpass = spark.read.parquet(mpass_path)

# === Chuẩn schema đích (30+ cột) ===
schema_cols = [
    "STT", "Mã KH", "Mã HĐ", "NV quản lý", "Mã nhân viên", "Mã lái xe",
    "ID cuốc xe", "Số thẻ", "Tên trên thẻ", "Sản phẩm", "Số tiền",
    "Số giao dịch", "Mã giao dịch", "Mã tài", "Ngày giao dịch", "Thời điểm giao dịch",
    "Ngày đi thực tế", "Ngày hiệu lực", "Biển số xe", "Điểm đón", "Điểm trả",
    "Đơn vị phát sinh", "Mã ĐV phát sinh", "Tên ĐV phát sinh", "Mã ĐV quản lý KH",
    "Tên ĐV quản lý KH", "Trạng thái giao dịch", "Loại giao dịch", "Ghi chú"
]

# === Ánh xạ MCC ===
df_mcc_norm = df_mcc.select(
    F.col("stt").alias("STT"),
    F.lit(None).cast("string").alias("Mã KH"),
    F.lit(None).cast("string").alias("Mã HĐ"),
    F.col("ma_nv").alias("NV quản lý"),
    F.lit(None).cast("string").alias("Mã nhân viên"),
    F.lit(None).cast("string").alias("Mã lái xe"),
    F.lit(None).cast("string").alias("ID cuốc xe"),
    F.col("so_the").alias("Số thẻ"),
    F.col("ten_in_tren_the").alias("Tên trên thẻ"),
    F.col("san_pham").alias("Sản phẩm"),
    F.col("so_tien").cast("double").alias("Số tiền"),
    F.col("so_giao_dich").alias("Số giao dịch"),
    F.lit(None).cast("string").alias("Mã giao dịch"),
    F.lit(None).cast("string").alias("Mã tài"),
    (F.col("ngay_giao_dich") if "ngay_giao_dich" in df_mcc.columns else F.lit(None).cast("string")).alias("Ngày giao dịch"),
    F.lit(None).cast("string").alias("Thời điểm giao dịch"),
    F.col("ngay_di_thuc_te").alias("Ngày đi thực tế"),
    F.col("ngay_hieu_luc").alias("Ngày hiệu lực"),
    F.col("bs_xe").alias("Biển số xe"),
    F.col("diem_don_ghi_chu").alias("Điểm đón"),
    F.col("diem_tra").alias("Điểm trả"),
    F.col("don_vi_phat_sinh").alias("Đơn vị phát sinh"),
    F.lit(None).cast("string").alias("Mã ĐV phát sinh"),
    F.lit(None).cast("string").alias("Tên ĐV phát sinh"),
    F.lit(None).cast("string").alias("Mã ĐV quản lý KH"),
    F.lit(None).cast("string").alias("Tên ĐV quản lý KH"),
    F.lit(None).cast("string").alias("Trạng thái giao dịch"),
    F.lit(None).cast("string").alias("Loại giao dịch"),
    F.lit(None).cast("string").alias("Ghi chú")
)

# === Ánh xạ Mpass ===
df_mpass_norm = df_mpass.select(
    F.col("stt").alias("STT"),
    F.col("ma_kh").alias("Mã KH"),
    F.col("ma_hd").alias("Mã HĐ"),
    F.col("nv_quan_ly").alias("NV quản lý"),
    F.lit(None).cast("string").alias("Mã nhân viên"),
    F.col("ma_lai_xe").alias("Mã lái xe"),
    F.col("id_cuoc_xe").alias("ID cuốc xe"),
    F.col("so_the").alias("Số thẻ"),
    F.col("ten_in_tren_the").alias("Tên trên thẻ"),
    F.col("san_pham").alias("Sản phẩm"),
    F.col("so_tien").cast("double").alias("Số tiền"),
    F.lit(None).cast("string").alias("Số giao dịch"),
    F.col("ma_gd").alias("Mã giao dịch"),
    F.col("so_tai").alias("Mã tài"),
    F.lit(None).cast("string").alias("Ngày giao dịch"),
    F.col("thoi_diem_giao_dich").alias("Thời điểm giao dịch"),
    F.lit(None).cast("string").alias("Ngày đi thực tế"),
    F.lit(None).cast("string").alias("Ngày hiệu lực"),
    F.lit(None).cast("string").alias("Biển số xe"),
    F.lit(None).cast("string").alias("Điểm đón"),
    F.lit(None).cast("string").alias("Điểm trả"),
    F.lit(None).cast("string").alias("Đơn vị phát sinh"),
    F.col("ma_dv_phat_sinh_gd").alias("Mã ĐV phát sinh"),
    F.col("ten_dv_phat_sinh_gd").alias("Tên ĐV phát sinh"),
    F.lit(None).cast("string").alias("Mã ĐV quản lý KH"),
    F.lit(None).cast("string").alias("Tên ĐV quản lý KH"),
    F.col("trang_thai").alias("Trạng thái giao dịch"),
    F.col("loai_gd").alias("Loại giao dịch"),
    F.lit(None).cast("string").alias("Ghi chú")
)

# === Union 2 bảng cùng schema ===
df_union = df_mcc_norm.unionByName(df_mpass_norm)

print("=== Schema hợp nhất ===")
df_union.printSchema()
print("=== Một số dòng dữ liệu ===")
df_union.show(30, truncate=False)

# === Ghi kết quả ra MinIO (1 file duy nhất) ===
output_path = "s3a://warehouse/silver/merged"

# coalesce(1): gom tất cả partitions về 1 → tạo 1 file output
df_union.coalesce(1).write.mode("overwrite").parquet(output_path)

print(f"✅ Đã lưu dữ liệu hợp nhất (1 file) vào: {output_path}")


spark.stop()
