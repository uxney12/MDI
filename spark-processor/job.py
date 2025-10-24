import logging
import sys
import traceback
import re
from datetime import datetime
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.types import StringType, IntegerType, DateType, TimestampType



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


spark = (
    SparkSession.builder
    .appName("SparkProcessorBronzeToSilver")
    .config("spark.sql.catalog.minio", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.minio.type", "hadoop")
    .config("spark.sql.catalog.minio.warehouse", "s3a://lakehouse/")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

logger.info("started successfully")

def filter_deleted_data(df):
    cols_to_keep = [c for c in df.columns if "deleted" not in c.lower()]
    df = df.select(cols_to_keep)
    if "deleted" in df.columns:
        df = df.filter((F.col("deleted") != True) | F.col("deleted").isNull())
    return df


@F.udf(returnType=IntegerType())
def clean_number(value):
    if value is None:
        return None
    s = str(value).replace(",", "").replace(".", "").replace(" ", "").strip()
    return int(s) if s.isdigit() else None


@F.udf(returnType=StringType())
def clean_date(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.strftime("%d-%m-%Y")

    s = str(value).strip()
    if not s:
        return None

    for sep in ["/", ".", " "]:
        s = s.replace(sep, "-")

    parts = s.split("-")
    if len(parts) != 3:
        return None

    try:
        p1, p2, p3 = map(int, parts)
    except ValueError:
        return None

    if 1000 <= p1 <= 9999:  
        y, m, d = p1, p2, p3
    elif 1000 <= p3 <= 9999:  
        y, m, d = p3, p2, p1
    else:
        return None

    try:
        return datetime(y, m, d).strftime("%d-%m-%Y")
    except ValueError:
        return None


@F.udf(returnType=StringType())
def clean_whitespace(value):
    if not value:
        return None
    return re.sub(r"\s+", " ", str(value).strip())


@F.udf(returnType=StringType())
def clean_code(value):
    if not value:
        return None
    s = re.sub(r"[^0-9A-Za-zÀ-ỹ]", "", str(value))
    return s.upper() if not s.isdigit() else s

tables = {
    "mcc": "s3a://lakehouse/bronze/MDI/mcc/data/",
    "mpass": "s3a://lakehouse/bronze/Mpass/mpass/data/",
}

silver_path = "s3a://lakehouse/silver/data/"

for name, path in tables.items():
    try:
        df = spark.read.parquet(path)
        df = filter_deleted_data(df)
        logger.info(f"Đọc thành công {name}, số dòng sau lọc: {df.count()}")

        df.show(3, truncate=False)
        globals()[f"df_{name}"] = df

    except Exception as e:
        logger.error(f"Lỗi khi đọc bảng {name}: {e}")
        logger.error(traceback.format_exc())


# =======================
# Chuẩn hóa bảng MCC
# =======================
df_mcc = globals().get("df_mcc")
if df_mcc:
    df_mcc_norm = df_mcc.select(
        F.col("stt").cast(StringType()).alias("STT"),
        F.lit(None).cast(StringType()).alias("Mã KH"),
        F.lit(None).cast(StringType()).alias("Mã HĐ"),
        clean_code(F.col("ma_nv")).alias("Mã nhân viên"),
        F.lit(None).cast(StringType()).alias("NV quản lý"),
        F.lit(None).cast(StringType()).alias("Mã lái xe"),
        F.lit(None).cast(StringType()).alias("ID cuốc xe"),
        F.col("so_the").alias("Số thẻ"),
        clean_whitespace(F.col("ten_in_tren_the")).alias("Tên trên thẻ"),
        clean_whitespace(F.col("san_pham")).alias("Sản phẩm"),
        clean_number(F.col("so_tien")).alias("Số tiền"),
        clean_code(F.col("so_giao_dich")).alias("Số giao dịch"),
        F.lit(None).cast(StringType()).alias("Mã giao dịch"),
        F.lit(None).cast(StringType()).alias("Mã tài"),
        F.lit(None).cast(DateType()).alias("Ngày giao dịch"),
        F.lit(None).cast(DateType()).alias("Thời điểm giao dịch"),
        clean_date(F.col("ngay_di_thuc_te")).alias("Ngày đi thực tế"),
        clean_date(F.col("ngay_hieu_luc")).alias("Ngày hiệu lực"),
        clean_code(F.col("bs_xe")).alias("Biển số xe"),
        clean_whitespace(F.col("diem_don_ghi_chu")).alias("Điểm đón"),
        clean_whitespace(F.col("diem_tra")).alias("Điểm trả"),
        clean_whitespace(F.col("don_vi_phat_sinh")).alias("Đơn vị phát sinh"),
        F.lit(None).cast(StringType()).alias("Mã ĐV phát sinh"),
        F.lit(None).cast(StringType()).alias("Tên ĐV phát sinh"),
        F.lit(None).cast(StringType()).alias("Mã ĐV quản lý KH"),
        F.lit(None).cast(StringType()).alias("Tên ĐV quản lý KH"),
        F.lit(None).cast(StringType()).alias("Trạng thái giao dịch"),
        F.lit(None).cast(StringType()).alias("Loại giao dịch"),
        F.lit(None).cast(StringType()).alias("Ghi chú")
    )

# =======================
# Chuẩn hóa bảng MPASS
# =======================
df_mpass = globals().get("df_mpass")
if df_mpass:
    df_mpass_norm = df_mpass.select(
        F.col("stt").cast(IntegerType()).alias("STT"),
        clean_code(F.col("ma_kh")).alias("Mã KH"),
        clean_code(F.col("ma_hd")).alias("Mã HĐ"),
        clean_code(F.col("nv_quan_ly")).alias("NV quản lý"),
        F.lit(None).cast(StringType()).alias("Mã nhân viên"),
        clean_code(F.col("ma_lai_xe")).alias("Mã lái xe"),
        clean_code(F.col("id_cuoc_xe")).alias("ID cuốc xe"),
        F.col("so_the").alias("Số thẻ"),
        clean_whitespace(F.col("ten_in_tren_the")).alias("Tên trên thẻ"),
        clean_whitespace(F.col("san_pham")).alias("Sản phẩm"),
        clean_number(F.col("so_tien")).alias("Số tiền"),
        F.lit(None).cast(StringType()).alias("Số giao dịch"),
        clean_code(F.col("ma_gd")).alias("Mã giao dịch"),
        clean_code(F.col("so_tai")).alias("Mã tài"),
        F.lit(None).cast(DateType()).alias("Ngày giao dịch"),
        clean_date(F.col("thoi_diem_giao_dich")).alias("Thời điểm giao dịch"),
        F.lit(None).cast(DateType()).alias("Ngày đi thực tế"),
        F.lit(None).cast(DateType()).alias("Ngày hiệu lực"),
        F.lit(None).cast(StringType()).alias("Biển số xe"),
        F.lit(None).cast(StringType()).alias("Điểm đón"),
        F.lit(None).cast(StringType()).alias("Điểm trả"),
        F.lit(None).cast(StringType()).alias("Đơn vị phát sinh"),
        clean_code(F.col("ma_dv_phat_sinh_gd")).alias("Mã ĐV phát sinh"),
        clean_whitespace(F.col("ten_dv_phat_sinh_gd")).alias("Tên ĐV phát sinh"),
        F.lit(None).cast(StringType()).alias("Mã ĐV quản lý KH"),
        F.lit(None).cast(StringType()).alias("Tên ĐV quản lý KH"),
        clean_whitespace(F.col("trang_thai")).alias("Trạng thái giao dịch"),
        clean_whitespace(F.col("loai_gd")).alias("Loại giao dịch"),
        F.lit(None).cast(StringType()).alias("Ghi chú")
    )



if 'df_mcc_norm' in locals() and 'df_mpass_norm' in locals():
    df_silver = df_mcc_norm.unionByName(df_mpass_norm)

    df_silver = df_silver.coalesce(1)

    df_silver.write.mode("overwrite").parquet(silver_path)

    logger.info(f"✅ Lưu dữ liệu Silver thành công: {silver_path}")
    logger.info(f"📌 Tổng số dòng: {df_silver.count()}")
else:
    logger.error("Lỗi")

