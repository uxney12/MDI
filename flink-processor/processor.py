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
        self.data_bucket = os.getenv("DATA_BUCKET", "iceberg-data")

        self.consumer = None
        self.record_count = 0
        
        # Cache để lưu schema của từng table
        self.schema_cache = {}

        logger.info("=== Flink Processor Configuration ===")
        logger.info(f"Kafka: {self.kafka_servers}")
        logger.info(f"Flink JobManager: {self.flink_host}:{self.flink_port}")
        logger.info(f"MinIO Endpoint: {self.s3_endpoint}")
        logger.info(f"Data Bucket: {self.data_bucket}")
        logger.info("Auto-detecting schemas from incoming data...")

        # Setup S3 Client
        self.s3_client = self.setup_s3_client()
        
        # Create MinIO bucket if not exists
        self.ensure_bucket_exists()

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
            try:
                self.s3_client.head_bucket(Bucket=self.data_bucket)
                logger.info(f"✓ Bucket '{self.data_bucket}' already exists")
            except ClientError:
                self.s3_client.create_bucket(Bucket=self.data_bucket)
                logger.info(f"✓ Created bucket '{self.data_bucket}'")
                
        except Exception as e:
            logger.error(f"❌ Could not verify/create bucket: {e}")
            raise

    # =========================
    # Get Parquet Path for Table
    # =========================
    def get_parquet_path(self, database, table):
        """Generate parquet file path based on database and table."""
        return f"{database}/{table}/{table}_data.parquet"

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
        """Create Kafka consumer for both PostgreSQL and MS SQL topics."""
        try:
            self.consumer = KafkaConsumer(
                "postgres-mcc-changes",
                "mssql-mpass-changes",
                bootstrap_servers=self.kafka_servers.split(","),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="flink-processor-group",
            )
            logger.info("✓ Connected to Kafka consumer")
            logger.info("  Topics: postgres-mcc-changes, mssql-mpass-changes")
        except KafkaError as e:
            logger.error(f"Kafka connection failed: {e}")
            raise

    # =========================
    # Auto-detect and Cache Schema
    # =========================
    def get_or_create_schema(self, data_dict, table_key):
        """Auto-detect schema from data and cache it."""
        if table_key in self.schema_cache:
            # Kiểm tra xem có cột mới không
            existing_fields = set(self.schema_cache[table_key].names)
            new_fields = set(data_dict.keys())
            
            if new_fields - existing_fields:
                logger.info(f"Detected new columns for {table_key}: {new_fields - existing_fields}")
                # Tạo lại schema với cột mới
                self.schema_cache[table_key] = self._build_schema(data_dict)
            
            return self.schema_cache[table_key]
        else:
            # Tạo schema mới
            schema = self._build_schema(data_dict)
            self.schema_cache[table_key] = schema
            logger.info(f"✓ Created schema for {table_key} with {len(schema.names)} columns")
            logger.debug(f"  Columns: {', '.join(schema.names)}")
            return schema

    def _build_schema(self, data_dict):
        """Build PyArrow schema from dictionary."""
        fields = []
        
        # Sort keys để đảm bảo thứ tự cố định
        for key in sorted(data_dict.keys()):
            fields.append((key, pa.string()))
        
        return pa.schema(fields)

    # =========================
    # Generate Unique Record ID
    # =========================
    def generate_record_id(self, data, table_name, database_name):
        """Generate unique ID from primary key or hash."""
        # Thử các trường primary key phổ biến
        pk_fields = ['id', 'stt', 'ma_gd', 'ma_giao_dich', 'ma_kh']
        
        for field in pk_fields:
            if field in data and data[field] and str(data[field]).strip():
                return f"{database_name}_{table_name}_{data[field]}"
        
        # Nếu không có PK, dùng hash
        import hashlib
        data_str = json.dumps(data, sort_keys=True)
        hash_id = hashlib.md5(data_str.encode()).hexdigest()[:16]
        return f"{database_name}_{table_name}_{hash_id}"

    # =========================
    # Transform Logic - Keep 100% Original Structure
    # =========================
    def transform_record(self, record):
        """Transform record by adding metadata, keeping ALL original columns."""
        try:
            # Lấy metadata từ record
            table_name = str(record.get("table", "")).lower()
            database_name = str(record.get("database", ""))
            source_type = str(record.get("source", ""))
            operation = str(record.get("operation", ""))
            data = record.get("data", {})
            
            # Parse JSON string nếu data là string
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse data JSON: {e}")
                    return None, None, None

            # Helper: convert any value to string safely
            def safe_str(value):
                if value is None:
                    return ''
                if isinstance(value, (dict, list)):
                    return json.dumps(value)
                return str(value)

            # Tạo transformed record với metadata + ALL original data
            transformed = {}
            
            # 1. Metadata fields (luôn ở đầu)
            transformed['_source_database'] = database_name
            transformed['_source_table'] = table_name
            transformed['_source_type'] = source_type
            transformed['_operation'] = operation
            transformed['_processed_at'] = datetime.now().isoformat()
            
            # 2. Auto-generate record_id
            record_id = self.generate_record_id(data, table_name, database_name)
            transformed['_record_id'] = record_id
            
            # 3. Copy TẤT CẢ các field từ data gốc (không filter, không hardcode)
            for key, value in data.items():
                transformed[key] = safe_str(value)
            
            return transformed, table_name, database_name

        except Exception as e:
            logger.error(f"Transform error: {e}")
            logger.exception(e)
            return None, None, None

    # =========================
    # Read Existing Parquet File
    # =========================
    def read_existing_data(self, parquet_path):
        """Read existing parquet file from MinIO."""
        try:
            import io
            
            response = self.s3_client.get_object(
                Bucket=self.data_bucket,
                Key=parquet_path
            )
            
            parquet_bytes = response['Body'].read()
            buffer = io.BytesIO(parquet_bytes)
            table = pq.read_table(buffer)
            
            logger.info(f"✓ Read {len(table)} records from {parquet_path}")
            return table
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                logger.info(f"No existing file at {parquet_path}, will create new")
                return None
            else:
                raise
        except Exception as e:
            logger.error(f"Error reading {parquet_path}: {e}")
            return None

    # =========================
    # Update Parquet File with Auto Schema
    # =========================
    def update_parquet_file(self, new_record, parquet_path, table_key):
        """Update parquet file with auto-detected schema."""
        try:
            import io

            existing_table = self.read_existing_data(parquet_path)
            df_new = pd.DataFrame([new_record])

            if existing_table is not None:
                df_existing = existing_table.to_pandas()

                # Merge columns từ cả 2 dataframes
                all_columns = sorted(set(df_existing.columns) | set(df_new.columns))
                
                # Đảm bảo cả 2 df có đủ cột
                for col in all_columns:
                    if col not in df_existing.columns:
                        df_existing[col] = ''
                        logger.info(f"  Added new column to existing data: {col}")
                    if col not in df_new.columns:
                        df_new[col] = ''
                
                # Sắp xếp cột theo thứ tự
                df_existing = df_existing[all_columns]
                df_new = df_new[all_columns]

                # Use _record_id as index
                df_existing['_record_id'] = df_existing['_record_id'].astype(str)
                df_new['_record_id'] = df_new['_record_id'].astype(str)
                df_existing.set_index('_record_id', inplace=True, drop=False)
                df_new.set_index('_record_id', inplace=True, drop=False)

                record_id = str(df_new.index[0])
                operation = new_record.get('_operation', '').upper()

                if record_id in df_existing.index:
                    if operation == 'DELETE':
                        df_existing = df_existing.drop(record_id, errors='ignore')
                        logger.info(f"  Deleted: {record_id}")
                    else:
                        for col in df_existing.columns:
                            df_existing.at[record_id, col] = df_new.at[record_id, col]
                        logger.info(f"  Updated: {record_id}")
                    df_combined = df_existing.reset_index(drop=True)
                else:
                    if operation == 'DELETE':
                        logger.warning(f"  Delete for non-existing ID: {record_id}")
                        df_combined = df_existing.reset_index(drop=True)
                    else:
                        df_combined = pd.concat(
                            [df_existing.reset_index(drop=True), df_new.reset_index(drop=True)],
                            ignore_index=True
                        )
                        logger.info(f"  Inserted: {record_id}")
            else:
                df_combined = pd.DataFrame([new_record])
                logger.info(f"  Created first record: {new_record.get('_record_id')}")

            # Convert all to string
            for col in df_combined.columns:
                df_combined[col] = df_combined[col].astype(str)

            # Auto-detect schema
            if len(df_combined) > 0:
                sample_record = df_combined.iloc[0].to_dict()
                arrow_schema = self.get_or_create_schema(sample_record, table_key)
            else:
                arrow_schema = self.get_or_create_schema(new_record, table_key)

            # Write parquet
            arrow_table = pa.Table.from_pandas(df_combined, schema=arrow_schema)
            buffer = io.BytesIO()
            pq.write_table(arrow_table, buffer, compression='snappy')
            buffer.seek(0)

            self.s3_client.put_object(
                Bucket=self.data_bucket,
                Key=parquet_path,
                Body=buffer.getvalue()
            )

            logger.info(f"✓ Saved: s3://{self.data_bucket}/{parquet_path} ({len(df_combined)} records, {len(arrow_schema.names)} columns)")

        except Exception as e:
            logger.error(f"Error updating parquet: {e}")
            logger.exception(e)
            raise

    # =========================
    # Process Stream
    # =========================
    def process(self):
        """Main streaming process with auto schema detection."""
        self.wait_for_services()
        self.connect_kafka()
        
        logger.info("=" * 60)
        logger.info("🚀 Start consuming with AUTO SCHEMA DETECTION")
        logger.info("  All columns from source tables will be preserved")
        logger.info("=" * 60)

        for message in self.consumer:
            try:
                self.record_count += 1
                record = message.value
                
                source_info = f"{record.get('source')}.{record.get('database')}.{record.get('table')}"
                logger.info(f"\n[{self.record_count}] Received from {source_info}")
                
                # Transform (giữ 100% cấu trúc gốc + metadata)
                transformed, table_name, database_name = self.transform_record(record)
                
                if transformed and table_name and database_name:
                    # Tạo path động
                    parquet_path = self.get_parquet_path(database_name, table_name)
                    table_key = f"{database_name}.{table_name}"
                    
                    # Update parquet với auto schema
                    self.update_parquet_file(transformed, parquet_path, table_key)
                    
                    logger.info(
                        f"[{self.record_count}] ✓ Processed | "
                        f"Op: {transformed['_operation']:10} | "
                        f"Cols: {len(transformed)} | "
                        f"Path: {parquet_path}"
                    )
                else:
                    logger.warning(f"[{self.record_count}] Failed to transform")
                    
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