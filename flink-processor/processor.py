import os
import json
import time
import logging
import socket
import requests
import pandas as pd
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

# =========================
# Logging Configuration
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("FlinkProcessor")

# =========================
# Processor Class
# =========================
class FlinkProcessor:
    def __init__(self):
        # Kafka + Flink
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "jobmanager")
        self.flink_port = os.getenv("FLINK_JOBMANAGER_PORT", "8081")

        # MinIO credentials
        self.s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        self.s3_access_key = os.getenv("S3_ACCESS_KEY", "admin")
        self.s3_secret_key = os.getenv("S3_SECRET_KEY", "password123")
        self.data_bucket = os.getenv("DATA_BUCKET", "bangke-data")
        
        # Single parquet file path
        self.parquet_file_key = "bangke/bangke_data.parquet"

        self.consumer = None
        self.record_count = 0

        logger.info("=== Flink Processor Configuration ===")
        logger.info(f"Kafka: {self.kafka_servers}")
        logger.info(f"Flink JobManager: {self.flink_host}:{self.flink_port}")
        logger.info(f"MinIO Endpoint: {self.s3_endpoint}")
        logger.info(f"Data Bucket: {self.data_bucket}")
        logger.info(f"Parquet File: s3://{self.data_bucket}/{self.parquet_file_key}")

        # Setup S3 Client
        self.s3_client = self.setup_s3_client()
        
        # Create MinIO bucket if not exists
        self.ensure_bucket_exists()
        
        # Initialize data schema
        self.arrow_schema = self.get_arrow_schema()

    # =========================
    # Setup S3 Client
    # =========================
    def setup_s3_client(self):
        """Initialize boto3 S3 client for MinIO."""
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                config=Config(signature_version='s3v4'),
                region_name='us-east-1'
            )
            logger.info("✓ S3 client initialized successfully")
            return s3_client
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 client: {e}")
            raise

    # =========================
    # Ensure MinIO Bucket Exists
    # =========================
    def ensure_bucket_exists(self):
        """Create MinIO bucket if it doesn't exist."""
        try:
            # Check if bucket exists
            try:
                self.s3_client.head_bucket(Bucket=self.data_bucket)
                logger.info(f"✓ Bucket '{self.data_bucket}' already exists")
            except ClientError:
                # Create bucket if not exists
                self.s3_client.create_bucket(Bucket=self.data_bucket)
                logger.info(f"✓ Created bucket '{self.data_bucket}'")
                
        except Exception as e:
            logger.error(f"❌ Could not verify/create bucket: {e}")
            raise

    # =========================
    # Define Arrow Schema
    # =========================
    def get_arrow_schema(self):
        """Define PyArrow schema for the parquet file."""
        return pa.schema([
            ('id', pa.string()),  # Unique identifier for each record
            ('source_table', pa.string()),
            ('operation', pa.string()),
            ('processed_at', pa.string()),
            ('stt', pa.string()),
            ('ma_kh', pa.string()),
            ('ma_hd', pa.string()),
            ('nv_quan_ly', pa.string()),
            ('ma_nhan_vien', pa.string()),
            ('ma_lai_xe', pa.string()),
            ('id_cuoc_xe', pa.string()),
            ('so_the', pa.string()),
            ('ten_tren_the', pa.string()),
            ('san_pham', pa.string()),
            ('so_tien', pa.string()),
            ('so_giao_dich', pa.string()),
            ('ma_giao_dich', pa.string()),
            ('ma_tai', pa.string()),
            ('ngay_giao_dich', pa.string()),
            ('thoi_diem_giao_dich', pa.string()),
            ('ngay_di_thuc_te', pa.string()),
            ('ngay_hieu_luc', pa.string()),
            ('bien_so_xe', pa.string()),
            ('diem_don', pa.string()),
            ('diem_tra', pa.string()),
            ('don_vi_phat_sinh', pa.string()),
            ('ma_dv_phat_sinh', pa.string()),
            ('ten_dv_phat_sinh', pa.string()),
            ('ma_dv_quan_ly_kh', pa.string()),
            ('ten_dv_quan_ly_kh', pa.string()),
            ('trang_thai_giao_dich', pa.string()),
            ('loai_giao_dich', pa.string()),
            ('ghi_chu', pa.string()),
            ('is_deleted', pa.bool_()),  # Soft delete flag
        ])

    # =========================
    # Service Health Check
    # =========================
    def wait_for_services(self):
        """Wait until Kafka and Flink are available."""
        logger.info("Waiting for Kafka...")
        host, port = self.kafka_servers.split(":")
        for i in range(30):
            try:
                s = socket.create_connection((host, int(port)), timeout=2)
                s.close()
                logger.info("✓ Kafka is ready")
                break
            except Exception:
                time.sleep(3)
                if i == 29:
                    raise Exception("Kafka not available after retries")

        logger.info("Waiting for Flink JobManager...")
        for i in range(20):
            try:
                r = requests.get(f"http://{self.flink_host}:{self.flink_port}/overview", timeout=3)
                if r.status_code == 200:
                    logger.info("✓ Flink JobManager is ready")
                    return
            except:
                time.sleep(3)
        logger.warning("⚠ Flink not ready, continuing anyway...")

    # =========================
    # Kafka Consumer
    # =========================
    def connect_kafka(self):
        """Create Kafka consumer."""
        try:
            self.consumer = KafkaConsumer(
                "mcc-changes",
                "mpass-changes",
                bootstrap_servers=self.kafka_servers.split(","),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="flink-processor-group",
            )
            logger.info("✓ Connected to Kafka consumer")
        except KafkaError as e:
            logger.error(f"Kafka connection failed: {e}")
            raise

    # =========================
    # Generate Unique Record ID
    # =========================
    def generate_record_id(self, record):
        table = str(record.get('source_table') or record.get('table') or '').upper()
        stt = str(record.get('stt') or '')
        return f"{table}_{stt}"


    # =========================
    # Transform Logic
    # =========================
    def transform_to_bangke(self, record):
        """Transform record from MCC/MPASS → BangKe."""
        try:
            table_name = record.get("table")
            operation = record.get("operation")
            data = record.get("data", {})

            # Convert to ISO format string
            processed_at = datetime.now().isoformat()

            # Helper function to safely convert to string
            def safe_str(value):
                return str(value) if value is not None else ''

            # Initialize with empty strings
            bangke = {
                'source_table': str(table_name or ''),
                'operation': str(operation or ''),
                'processed_at': processed_at,
                'stt': '',
                'ma_kh': '',
                'ma_hd': '',
                'nv_quan_ly': '',
                'ma_nhan_vien': '',
                'ma_lai_xe': '',
                'id_cuoc_xe': '',
                'so_the': '',
                'ten_tren_the': '',
                'san_pham': '',
                'so_tien': '',
                'so_giao_dich': '',
                'ma_giao_dich': '',
                'ma_tai': '',
                'ngay_giao_dich': '',
                'thoi_diem_giao_dich': '',
                'ngay_di_thuc_te': '',
                'ngay_hieu_luc': '',
                'bien_so_xe': '',
                'diem_don': '',
                'diem_tra': '',
                'don_vi_phat_sinh': '',
                'ma_dv_phat_sinh': '',
                'ten_dv_phat_sinh': '',
                'ma_dv_quan_ly_kh': '',
                'ten_dv_quan_ly_kh': '',
                'trang_thai_giao_dich': '',
                'loai_giao_dich': '',
                'ghi_chu': '',
                'is_deleted': False
            }

            # Mapping MCC → BangKe
            if table_name == 'MCC':
                bangke.update({
                    'stt': safe_str(data.get('stt', '')),
                    'so_the': safe_str(data.get('so_the', '')),
                    'san_pham': safe_str(data.get('san_pham', '')),
                    'ngay_di_thuc_te': safe_str(data.get('ngay_di_thuc_te', '')),
                    'ngay_hieu_luc': safe_str(data.get('ngay_hieu_luc', '')),
                    'ten_tren_the': safe_str(data.get('ten_in_tren_the', '')),
                    'bien_so_xe': safe_str(data.get('bs_xe', '')),
                    'ma_nhan_vien': safe_str(data.get('ma_nv', '')),
                    'so_tien': safe_str(data.get('so_tien', '')),
                    'so_giao_dich': safe_str(data.get('so_giao_dich', '')),
                    'diem_don': safe_str(data.get('diem_don_ghi_chu', '')),
                    'diem_tra': safe_str(data.get('diem_tra', '')),
                    'don_vi_phat_sinh': safe_str(data.get('don_vi_phat_sinh', '')),
                    'ghi_chu': safe_str(data.get('ghi_chu', ''))
                })

            # Mapping MPASS → BangKe
            elif table_name == 'MPASS':
                bangke.update({
                    'stt': safe_str(data.get('stt', '')),
                    'ma_kh': safe_str(data.get('ma_kh', '')),
                    'ma_hd': safe_str(data.get('ma_hd', '')),
                    'nv_quan_ly': safe_str(data.get('nv_quan_ly', '')),
                    'ma_lai_xe': safe_str(data.get('ma_lai_xe', '')),
                    'id_cuoc_xe': safe_str(data.get('id_cuoc_xe', '')),
                    'so_the': safe_str(data.get('so_the', '')),
                    'ten_tren_the': safe_str(data.get('ten_in_tren_the', '')),
                    'san_pham': safe_str(data.get('san_pham', '')),
                    'so_tien': safe_str(data.get('so_tien', '')),
                    'ma_giao_dich': safe_str(data.get('ma_gd', '')),
                    'ma_tai': safe_str(data.get('so_tai', '')),
                    'thoi_diem_giao_dich': safe_str(data.get('thoi_diem_giao_dich', '')),
                    'ma_dv_phat_sinh': safe_str(data.get('ma_dv_phat_sinh_gd', '')),
                    'ten_dv_phat_sinh': safe_str(data.get('ten_dv_phat_sinh_gd', '')),
                    'trang_thai_giao_dich': safe_str(data.get('trang_thai', '')),
                    'loai_giao_dich': safe_str(data.get('loai_gd', ''))
                })

            # Generate unique ID
            bangke['id'] = self.generate_record_id(bangke)
            
            # Handle DELETE operation
            if operation and operation.upper() == 'DELETE':
                bangke['is_deleted'] = True

            return bangke

        except Exception as e:
            logger.error(f"Transform error: {e}")
            logger.exception(e)
            return None

    # =========================
    # Read Existing Parquet File
    # =========================
    def read_existing_data(self):
        """Read existing parquet file from MinIO."""
        try:
            import io
            
            # Download file from MinIO
            response = self.s3_client.get_object(
                Bucket=self.data_bucket,
                Key=self.parquet_file_key
            )
            
            # Read parquet file
            parquet_bytes = response['Body'].read()
            buffer = io.BytesIO(parquet_bytes)
            table = pq.read_table(buffer)
            
            logger.info(f"✓ Read existing data: {len(table)} records")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info("No existing parquet file found, will create new one")
                return None
            else:
                raise
        except Exception as e:
            logger.error(f"Error reading existing data: {e}")
            return None

    # =========================
    # Update Parquet File
    # =========================
    def update_parquet_file(self, new_record):
        try:
            import io

            existing_table = self.read_existing_data()
            df_new = pd.DataFrame([new_record])

            if existing_table is not None:
                df_existing = existing_table.to_pandas()

                # --- 1) đảm bảo df_new có đầy đủ cột ---
                for col in df_existing.columns:
                    if col not in df_new.columns:
                        df_new[col] = '' if col != 'is_deleted' else False
                for col in df_new.columns:
                    if col not in df_existing.columns:
                        df_existing[col] = '' if col != 'is_deleted' else False
                df_new = df_new[df_existing.columns]

                # --- 2) dùng index theo 'id' ---
                df_existing['id'] = df_existing['id'].astype(str)
                df_new['id'] = df_new['id'].astype(str)
                df_existing.set_index('id', inplace=True, drop=False)
                df_new.set_index('id', inplace=True, drop=False)

                record_id = str(df_new.index[0])
                operation = new_record.get('operation', '').upper()

                if record_id in df_existing.index:
                    if operation == 'DELETE':
                        # ✅ HARD DELETE: xóa hẳn dòng khỏi DataFrame
                        df_existing = df_existing.drop(record_id, errors='ignore')
                        logger.info(f"✓ Hard deleted record: {record_id}")
                    else:
                        # UPDATE
                        for col in df_existing.columns:
                            df_existing.at[record_id, col] = df_new.at[record_id, col]
                        logger.info(f"✓ Updated existing record: {record_id}")

                    df_combined = df_existing.reset_index(drop=True)

                else:
                    if operation == 'DELETE':
                        # Không chèn bản ghi mới nếu xóa mà không tồn tại
                        logger.warning(f"⚠ Delete requested for non-existing ID: {record_id} — skipping.")
                        df_combined = df_existing.reset_index(drop=True)
                    else:
                        # INSERT
                        df_combined = pd.concat(
                            [df_existing.reset_index(drop=True), df_new.reset_index(drop=True)],
                            ignore_index=True
                        )
                        logger.info(f"✓ Inserted new record: {record_id}")

            else:
                # Chưa có file parquet cũ → tạo mới
                df_combined = pd.DataFrame([new_record])
                logger.info(f"✓ Created first record: {new_record.get('id')}")

            # --- 3) Chuẩn hóa kiểu dữ liệu ---
            for col in df_combined.columns:
                if col == 'is_deleted':
                    df_combined[col] = df_combined[col].astype(bool)
                else:
                    df_combined[col] = df_combined[col].astype(str)

            # --- 4) Ghi lại parquet ---
            arrow_table = pa.Table.from_pandas(df_combined, schema=self.arrow_schema)
            buffer = io.BytesIO()
            pq.write_table(arrow_table, buffer, compression='snappy')
            buffer.seek(0)

            self.s3_client.put_object(
                Bucket=self.data_bucket,
                Key=self.parquet_file_key,
                Body=buffer.getvalue()
            )

            logger.info(f"✓ Updated parquet file: s3://{self.data_bucket}/{self.parquet_file_key}")
            logger.info(f"Total records in file: {len(df_combined)}")

        except Exception as e:
            logger.error(f"Error updating parquet file: {e}")
            logger.exception(e)
            raise


    # =========================
    # Process Stream
    # =========================
    def process(self):
        """Main streaming process."""
        self.wait_for_services()
        self.connect_kafka()
        
        logger.info("🚀 Start consuming Kafka messages...")

        for message in self.consumer:
            try:
                self.record_count += 1
                record = message.value
                
                logger.info(f"[{self.record_count}] Received message: {json.dumps(record)[:200]}")
                
                bangke_record = self.transform_to_bangke(record)
                
                if bangke_record:
                    self.update_parquet_file(bangke_record)
                    logger.info(
                        f"[{self.record_count}] ✓ "
                        f"{bangke_record['source_table']} | "
                        f"{bangke_record['operation']} | "
                        f"ID: {bangke_record['id']}"
                    )
                else:
                    logger.warning(f"[{self.record_count}] Failed to transform record")
                    
            except Exception as e:
                logger.error(f"Processing error: {e}")
                logger.exception(e)


# =========================
# Main Entry Point
# =========================
if __name__ == "__main__":
    try:
        processor = FlinkProcessor()
        processor.process()
    except KeyboardInterrupt:
        logger.info("Shutting down processor...")
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        logger.exception(e)
        raise