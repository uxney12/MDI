import os
import json
import time
import logging
import socket
import requests
import pandas as pd
import uuid
import io
from datetime import datetime
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from collections import defaultdict
from typing import Callable, Tuple

# =========================
# Logging configuration
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("IcebergBatchProcessor")

# =========================
# Processor
# =========================
class IcebergBatchProcessor:
    def __init__(self):
        self.kafka_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
        self.flink_host = os.getenv("FLINK_JOBMANAGER_HOST", "jobmanager")
        self.flink_port = os.getenv("FLINK_JOBMANAGER_PORT", "8081")

        # MinIO / S3
        self.s3_endpoint = os.getenv("S3_ENDPOINT", "http://minio:9000")
        self.s3_access_key = os.getenv("S3_ACCESS_KEY", "admin")
        self.s3_secret_key = os.getenv("S3_SECRET_KEY", "password123")
        self.warehouse_path = os.getenv("WAREHOUSE_PATH", "warehouse")

        self.batch_size = int(os.getenv("BATCH_SIZE", "500"))
        self.batch_timeout = int(os.getenv("BATCH_TIMEOUT", "30"))
        self.init_batch_size = int(os.getenv("INIT_BATCH_SIZE", "1000"))

        self.consumer = None
        self.record_count = 0

        self.batch_buffer = defaultdict(list)
        self.batch_timestamps = {}
        self.init_in_progress = set()


        self.schema_cache = {}
        self.table_metadata_cache = {}
        self.init_loaded_tables = set()

        logger.info("=== Iceberg Batch Processor (single file, init-batch + stream-per-record) ===")
        logger.info(f"Kafka: {self.kafka_servers}")
        logger.info(f"MinIO: {self.s3_endpoint}")
        logger.info(f"Warehouse: s3a://{self.warehouse_path}")
        logger.info(f"Batch Size: {self.batch_size} records")
        logger.info(f"Init Batch Size: {self.init_batch_size} records")

        # Setup S3 Client
        self.s3_client = self.setup_s3_client()
        self.ensure_bucket_exists(self.warehouse_path)
        self.is_init_mode = True

    # =========================
    # S3 helpers
    # =========================
    def setup_s3_client(self):
        try:
            s3_client = boto3.client(
                's3',
                endpoint_url=self.s3_endpoint,
                aws_access_key_id=self.s3_access_key,
                aws_secret_access_key=self.s3_secret_key,
                config=Config(signature_version='s3v4'),
                region_name=os.getenv('AWS_REGION', 'us-east-1')
            )
            logger.info("✓ S3 client initialized")
            return s3_client
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3: {e}")
            raise

    def ensure_bucket_exists(self, bucket_name):
        try:
            try:
                self.s3_client.head_bucket(Bucket=bucket_name)
                logger.info(f"✓ Bucket '{bucket_name}' exists")
            except ClientError:
                self.s3_client.create_bucket(Bucket=bucket_name)
                logger.info(f"✓ Created bucket '{bucket_name}'")
        except Exception as e:
            logger.error(f"❌ Bucket error: {e}")
            raise

    # =========================
    # Paths
    # =========================
    def get_table_base_path(self, database, table):
        return f"{database}/{table}"

    def get_metadata_path(self, database, table):
        return f"{self.get_table_base_path(database, table)}/metadata"

    def get_data_path(self, database, table):

    def get_data_file_key(self, database, table):
        return f"{self.get_data_path(database, table)}/{table}.parquet"

    # =========================
    # Metadata management (simplified)
    # =========================
    def load_table_metadata(self, database, table):
        table_key = f"{database}.{table}"
        if table_key in self.table_metadata_cache:
            return self.table_metadata_cache[table_key]

        version_hint_path = f"{self.get_metadata_path(database, table)}/version-hint.text"
        try:
            response = self.s3_client.get_object(Bucket=self.warehouse_path, Key=version_hint_path)
            current_version = int(response['Body'].read().decode('utf-8').strip())
            metadata_file_path = f"{self.get_metadata_path(database, table)}/v{current_version}.metadata.json"
            response = self.s3_client.get_object(Bucket=self.warehouse_path, Key=metadata_file_path)
            metadata = json.loads(response['Body'].read().decode('utf-8'))
            self.table_metadata_cache[table_key] = metadata
            return metadata
        except ClientError:
            metadata = self.create_new_table_metadata(database, table)
            self.table_metadata_cache[table_key] = metadata
            return metadata
        except Exception as e:
            logger.debug(f"Metadata load error: {e}")
            metadata = self.create_new_table_metadata(database, table)
            self.table_metadata_cache[table_key] = metadata
            return metadata

    def create_new_table_metadata(self, database, table):
        table_uuid = str(uuid.uuid4())
        metadata = {
            "format-version": 2,
            "table-uuid": table_uuid,
            "location": f"s3a://{self.warehouse_path}/{self.get_table_base_path(database, table)}",
            "last-updated-ms": int(datetime.now().timestamp() * 1000),
            "last-column-id": 0,
            "schemas": [],
            "current-schema-id": -1,
            "partition-specs": [{"spec-id": 0, "fields": []}],
            "default-spec-id": 0,
            "last-partition-id": 0,
            "properties": {"owner": "flink-processor", "created-at": datetime.now().isoformat()},
            "current-snapshot-id": -1,
            "snapshots": [],
            "snapshot-log": [],
            "metadata-log": [],
            "sort-orders": [],
            "refs": {}
        }
        return metadata

    def save_metadata(self, database, table, metadata):
        metadata_log_count = len(metadata.get('metadata-log', []))
        new_version = metadata_log_count + 1
        metadata['last-updated-ms'] = int(datetime.now().timestamp() * 1000)
        metadata['metadata-log'].append({
            "metadata-file": f"{self.get_metadata_path(database, table)}/v{new_version}.metadata.json",
            "timestamp-ms": metadata['last-updated-ms']
        })
        metadata_file_path = f"{self.get_metadata_path(database, table)}/v{new_version}.metadata.json"
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=metadata_file_path, Body=json.dumps(metadata, indent=2).encode('utf-8'))
        version_hint_path = f"{self.get_metadata_path(database, table)}/version-hint.text"
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=version_hint_path, Body=str(new_version).encode('utf-8'))
        logger.info(f"✓ Saved metadata v{new_version} for {database}.{table}")
        self.table_metadata_cache[f"{database}.{table}"] = metadata

    def add_schema_to_metadata(self, metadata, arrow_schema):
        schema_id = len(metadata['schemas'])
        last_column_id = metadata.get('last-column-id', 0)
        fields = []
        for f in arrow_schema:
            last_column_id += 1
            fields.append({"id": last_column_id, "name": f.name, "required": True, "type": "string"})
        metadata['schemas'].append({"schema-id": schema_id, "type": "struct", "fields": fields})
        metadata['current-schema-id'] = schema_id
        metadata['last-column-id'] = last_column_id
        return schema_id
    
    # =========================
    # Helper: normalize db/table names
    # =========================
    def normalize_db_table(self, database: str, table: str) -> Tuple[str,str]:

        if table is None:
            return database, table
        parts = table.split('.')
        normalized_table = parts[-1].strip().lower()
        normalized_db = database.strip() if database else database
        return normalized_db, normalized_table

    def atomic_write_data_file(self, database, table, bytes_data):
        final_key = self.get_data_file_key(database, table)
        temp_key = f"{self.get_data_path(database, table)}/._tmp_{uuid.uuid4().hex}.parquet"
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=temp_key, Body=bytes_data)
        copy_source = {'Bucket': self.warehouse_path, 'Key': temp_key}
        self.s3_client.copy_object(Bucket=self.warehouse_path, CopySource=copy_source, Key=final_key)
        self.s3_client.delete_object(Bucket=self.warehouse_path, Key=temp_key)
        return final_key

    # =========================
    # Schema management helper
    # =========================
    def df_to_arrow_schema(self, df):
        cols = sorted(df.columns.tolist())
        fields = [(c, pa.string()) for c in cols]
        return pa.schema(fields)

    # =========================
    # Read existing single data file (returns pyarrow.Table or None)
    # =========================
    def read_existing_table(self, database, table):
        key = self.get_data_file_key(database, table)
        try:
            resp = self.s3_client.get_object(Bucket=self.warehouse_path, Key=key)
            import io
            data = resp['Body'].read()
            buf = io.BytesIO(data)
            table = pq.read_table(buf)
            return table
        except ClientError as e:
            logger.debug(f"No data file for {database}.{table}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error reading existing table {database}.{table}: {e}")
            return None
        
    def commit_snapshot_and_logs(self, database, table, metadata, data_key, df_combined, change_entries, operation_summary, change_type='ddl'):
        try:
            change_log_path = None
            if change_entries:
                change_log_path = self.write_change_log(database, table, change_entries)

            file_size = 0
            try:
                resp = self.s3_client.head_object(Bucket=self.warehouse_path, Key=data_key)
                file_size = int(resp.get('ContentLength', 0))
            except Exception:
                file_size = operation_summary.get('file_size', 0)

            manifest_path = self.create_manifest_file(database, table, data_key, len(df_combined), file_size, operation_summary)

            manifest_list_path = self.create_manifest_list(database, table, [manifest_path], len(df_combined))

            snapshot_id = self.create_snapshot(metadata, manifest_list_path, operation_summary, change_log_path=change_log_path, change_type=change_type)

            self.save_metadata(database, table, metadata)

            logger.info(f"✓ Committed snapshot {snapshot_id} for {database}.{table} (change_type={change_type})")
            return {
                "change_log": change_log_path,
                "manifest": manifest_path,
                "manifest_list": manifest_list_path,
                "snapshot_id": snapshot_id
            }
        except Exception as e:
            logger.exception(f"Commit error for {database}.{table}: {e}")
            try:
                self.save_metadata(database, table, metadata)
            except Exception:
                pass
            raise

    def _write_back_table_with_schema(self, database, table, df):
        arrow_schema = self.df_to_arrow_schema(df)
        df = df[sorted(df.columns.tolist())]
        arrow_table = pa.Table.from_pandas(df, schema=arrow_schema)
        buf = io.BytesIO()
        pq.write_table(arrow_table, buf, compression='snappy')
        buf.seek(0)
        data_bytes = buf.getvalue()

        data_key = self.atomic_write_data_file(database, table, data_bytes)
        file_size = len(data_bytes)

        metadata = self.load_table_metadata(database, table)
        schema_id = self.add_schema_to_metadata(metadata, arrow_schema)

        change_entries = [{
            "record_id": None,
            "operation": "DDL",
            "before": None,
            "after": {"note": "ddl / schema write", "table": f"{database}.{table}", "schema_id": schema_id},
            "processed_at": datetime.now().isoformat()
        }]

        op_summary = {
            'batch_size': len(df),
            'inserts': len(df),
            'updates': 0,
            'deletes': 0,
            'total_records': len(df),
            'file_size': file_size,
            'primary_operation': 'ddl',
            'duration_ms': 0
        }

        return self.commit_snapshot_and_logs(database, table, metadata, data_key, df, change_entries, op_summary, change_type='ddl')


    def _rename_table_in_s3(self, database, old_table, new_table):
        prefix_old = f"{database}/{old_table}/"
        prefix_new = f"{database}/{new_table}/"
        response = self.s3_client.list_objects_v2(Bucket=self.warehouse_path, Prefix=prefix_old)
        for obj in response.get("Contents", []):
            old_key = obj["Key"]
            new_key = old_key.replace(prefix_old, prefix_new, 1)
            self.s3_client.copy_object(Bucket=self.warehouse_path, CopySource={"Bucket": self.warehouse_path, "Key": old_key}, Key=new_key)
            self.s3_client.delete_object(Bucket=self.warehouse_path, Key=old_key)
        logger.info(f"✓ Renamed S3 path {prefix_old} → {prefix_new}")


    # =========================
    # Logs: change-log (jsonl) and error-log
    # =========================
    def write_change_log(self, database, table, change_entries):
        ts = int(time.time() * 1000)
        key = f"{self.get_metadata_path(database, table)}/changes-{ts}.jsonl"
        payload = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in change_entries)
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=key, Body=payload)
        return key

    def write_error_log(self, database, table, error_entries):
        if not error_entries:
            return None
        ts = int(time.time() * 1000)
        key = f"{self.get_metadata_path(database, table)}/errors-{ts}.jsonl"
        payload = "".join(json.dumps(e, ensure_ascii=False) + "\n" for e in error_entries)
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=key, Body=payload)
        return key

    # =========================
    # Snapshot & manifest helpers
    # =========================
    def create_manifest_file(self, database, table, data_file_path, record_count, file_size, operation_summary):
        manifest_id = str(uuid.uuid4())
        manifest_path = f"{self.get_metadata_path(database, table)}/manifest-{manifest_id}.avro"
        manifest_entry = {
            "status": 1,
            "snapshot_id": None,
            "data_file": {
                "file_path": data_file_path,
                "file_format": "PARQUET",
                "record_count": record_count,
                "file_size_in_bytes": file_size,
                "partition": {},
                "batch_summary": operation_summary
            }
        }
        content = json.dumps([manifest_entry], indent=2)
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=manifest_path, Body=content.encode('utf-8'))
        return manifest_path

    def create_manifest_list(self, database, table, manifest_files, total_records):
        manifest_list_id = str(uuid.uuid4())
        manifest_list_path = f"{self.get_metadata_path(database, table)}/snap-{manifest_list_id}.avro"
        manifest_list = []
        for manifest_path in manifest_files:
            manifest_list.append({
                "manifest_path": manifest_path,
                "manifest_length": 0,
                "partition_spec_id": 0,
                "added_snapshot_id": None,
                "added_data_files_count": 1,
                "existing_data_files_count": 0,
                "deleted_data_files_count": 0,
                "added_rows_count": total_records,
                "existing_rows_count": 0,
                "deleted_rows_count": 0
            })
        content = json.dumps(manifest_list, indent=2)
        self.s3_client.put_object(Bucket=self.warehouse_path, Key=manifest_list_path, Body=content.encode('utf-8'))
        return manifest_list_path

    def create_snapshot(self, metadata, manifest_list_path, operation_summary, change_log_path=None, change_type='stream'):
        snapshot_id = int(time.time() * 1000)
        snapshot = {
            "snapshot-id": snapshot_id,
            "timestamp-ms": snapshot_id,
            "summary": {
                "type": change_type,
                "operation": operation_summary.get('primary_operation', ''),
                "total-records": operation_summary.get('total_records', 0),
                "file_size": operation_summary.get('file_size', 0),
                "batch-size": operation_summary.get('batch_size', 0),
                "batch-inserts": operation_summary.get('inserts', 0),
                "batch-updates": operation_summary.get('updates', 0),
                "batch-deletes": operation_summary.get('deletes', 0),
                "batch-duration-ms": operation_summary.get('duration_ms', 0),
                "change-log": change_log_path
            },
            "manifest-list": manifest_list_path,
            "schema-id": metadata.get('current-schema-id', -1)
        }
        metadata['snapshots'].append(snapshot)
        metadata['current-snapshot-id'] = snapshot_id
        metadata['refs']['main'] = {"snapshot-id": snapshot_id, "type": "branch"}
        metadata['snapshot-log'].append({"snapshot-id": snapshot_id, "timestamp-ms": snapshot_id})
        return snapshot_id

    # =========================
    # Record helpers
    # =========================
    def generate_record_id(self, data, table_name, database_name):
        pk_fields = ['stt']
        for field in pk_fields:
            if field in data and data[field] and str(data[field]).strip():
                return f"{database_name}_{table_name}_{data[field]}"
        import hashlib
        data_str = json.dumps(data, sort_keys=True)
        hash_id = hashlib.md5(data_str.encode()).hexdigest()[:16]
        return f"{database_name}_{table_name}_{hash_id}"

    def transform_record(self, record):
        try:
            table_name = str(record.get("table", "")).lower()
            database_name = str(record.get("database", ""))
            source_type = str(record.get("source", ""))
            operation = str(record.get("operation", "")).upper()
            data = record.get("data", {})
            if isinstance(data, str):
                data = json.loads(data)

            def safe_str(value):
                if value is None:
                    return ''
                if isinstance(value, (dict, list)):
                    return json.dumps(value, ensure_ascii=False)
                return str(value)

            transformed = {
                '_source_database': database_name,
                '_source_table': table_name,
                '_source_type': source_type,
                '_operation': operation,
                '_processed_at': datetime.now().isoformat()
            }

            for key, value in data.items():
                transformed[key] = safe_str(value)

            transformed['_record_id'] = self.generate_record_id(data, table_name, database_name)
            return transformed, table_name, database_name, operation
        except Exception as e:
            logger.error(f"Transform error: {e}")
            return None, None, None, None

    # =========================
    # INIT LOAD (batching): used only when no data exists for the table
    # =========================
    def process_init_batches(self, table_key, records):
        """Process records in batches (init). `records` is list of transformed dicts."""
        database, table = table_key.split('.')
        errors = []
        change_logs = []
        total_written = 0

        for i in range(0, len(records), self.batch_size):
            chunk = records[i:i + self.batch_size]
            try:
                df_chunk = pd.DataFrame(chunk).astype(str)

                if 'deleted' not in df_chunk.columns:
                    df_chunk['deleted'] = False

                existing_table = self.read_existing_table(database, table)
                if existing_table is not None:
                    df_existing = existing_table.to_pandas().astype(str)
                    all_columns = sorted(set(df_existing.columns) | set(df_chunk.columns))
                    for c in all_columns:
                        if c not in df_existing.columns:
                            df_existing[c] = ''
                        if c not in df_chunk.columns:
                            df_chunk[c] = ''
                    df_combined = pd.concat([df_existing, df_chunk], ignore_index=True)[all_columns]
                else:
                    df_combined = df_chunk

                arrow_schema = self.df_to_arrow_schema(df_combined)
                schema_changed = False
                metadata = self.load_table_metadata(database, table)
                if metadata['current-schema-id'] == -1 or metadata.get('schemas') == []:
                    self.add_schema_to_metadata(metadata, arrow_schema)
                    schema_changed = True

                import io

                for col in ['deleted']:
                    if col in df_combined.columns:
                        if col in arrow_schema.names:
                            field_type = arrow_schema.field(col).type
                            if pa.types.is_string(field_type):
                                df_combined[col] = df_combined[col].astype(str)
                            elif pa.types.is_boolean(field_type):
                                df_combined[col] = df_combined[col].astype(bool)
                        else:
                            df_combined[col] = df_combined[col].astype(bool)

                arrow_table = pa.Table.from_pandas(df_combined, schema=arrow_schema)
                buf = io.BytesIO()
                pq.write_table(arrow_table, buf, compression='snappy')
                buf.seek(0)
                bytes_data = buf.getvalue()
                data_key = self.atomic_write_data_file(database, table, bytes_data)
                file_size = len(bytes_data)

                batch_duration = 0 
                op_counts = {'INSERT': len(df_chunk), 'UPDATE': 0, 'DELETE': 0}
                op_summary = {
                    'batch_size': len(df_chunk),
                    'inserts': op_counts['INSERT'],
                    'updates': 0,
                    'deletes': 0,
                    'total_records': len(df_combined),
                    'file_size': file_size,
                    'primary_operation': 'batch',
                    'duration_ms': batch_duration
                }

                for r in chunk:
                    change_logs.append({
                        'record_id': r.get('_record_id'),
                        'operation': r.get('_operation', 'INSERT'),
                        'before': None,
                        'after': r,
                        'processed_at': datetime.now().isoformat()
                    })

                change_log_path = self.write_change_log(database, table, change_logs)
                manifest_path = self.create_manifest_file(database, table, data_key, len(df_combined), file_size, op_summary)
                manifest_list_path = self.create_manifest_list(database, table, [manifest_path], len(df_combined))
                self.create_snapshot(metadata, manifest_list_path, op_summary, change_log_path=change_log_path, change_type='batch')
                self.save_metadata(database, table, metadata)

                total_written += len(df_chunk)
                self.init_loaded_tables.add(table_key)

                change_logs = []

            except Exception as e:
                err = {"table": table_key, "error": str(e), "time": datetime.now().isoformat()}
                errors.append(err)
                logger.exception(f"Init batch error for {table_key}: {e}")

        if errors:
            self.write_error_log(database, table, errors)
        return total_written

    # =========================
    # STREAM per-record processing
    # - read existing table -> merge single record -> write back
    # - update metadata + change-log for each record
    # =========================
    def process_stream_record(self, table_key, record):
        database, table = table_key.split('.')
        errors = []
        before, after = None, None

        try:
            op = str(record.get('_operation', 'INSERT')).upper()
            rec_id = record.get('_record_id')

            existing_table = self.read_existing_table(database, table)
            if existing_table is not None:
                df_existing = existing_table.to_pandas().astype(str)
            else:
                df_existing = pd.DataFrame()

            if '_record_id' not in df_existing.columns:
                df_existing['_record_id'] = ''

            df_new = pd.DataFrame([record]).astype(str)
            if '_record_id' not in df_new.columns:
                df_new['_record_id'] = rec_id

            all_columns = sorted(set(df_existing.columns) | set(df_new.columns))
            for c in all_columns:
                if c not in df_existing.columns:
                    df_existing[c] = ''
                if c not in df_new.columns:
                    df_new[c] = ''

            df_existing.set_index('_record_id', inplace=True, drop=False)
            df_new.set_index('_record_id', inplace=True, drop=False)

            before = df_existing.loc[rec_id].to_dict() if rec_id in df_existing.index else None

            if op == 'DELETE':
                if rec_id in df_existing.index:
                    df_existing.at[rec_id, 'deleted'] = True
                    df_existing.at[rec_id, '_operation'] = "DELETE"
                    after = df_existing.loc[rec_id].to_dict()
                else:
                    after = None

            elif op == 'UPDATE':
                if rec_id in df_existing.index:
                    for col in df_new.columns:
                        if col not in ('_record_id', 'deleted'): 
                            df_existing.at[rec_id, col] = df_new.at[rec_id, col]
                    after = df_existing.loc[rec_id].to_dict()
                else:
                    df_new['deleted'] = False
                    df_existing = pd.concat([df_existing, df_new])
                    after = df_new.loc[rec_id].to_dict()

            elif op == 'INSERT':
                df_new['deleted'] = False
                df_existing = pd.concat([df_existing, df_new])
                after = df_new.loc[rec_id].to_dict()

            df_combined = df_existing.reset_index(drop=True).fillna('').astype(str)

            arrow_schema = self.df_to_arrow_schema(df_combined)
            metadata = self.load_table_metadata(database, table)
            schema_changed = False
            if metadata['current-schema-id'] == -1:
                self.add_schema_to_metadata(metadata, arrow_schema)
                schema_changed = True

            cached_schema_names = set(self.schema_cache.get(table_key, []).names) if table_key in self.schema_cache else set()
            current_schema_names = set(df_combined.columns)
            if table_key not in self.schema_cache or cached_schema_names != current_schema_names:
                schema_changed = True
                self.schema_cache[table_key] = pa.schema([(c, pa.string()) for c in sorted(current_schema_names)])
                self.add_schema_to_metadata(metadata, arrow_schema)

            import io
            arrow_table = pa.Table.from_pandas(df_combined, schema=self.schema_cache[table_key])
            buf = io.BytesIO()
            pq.write_table(arrow_table, buf, compression='snappy')
            buf.seek(0)
            bytes_data = buf.getvalue()
            data_key = self.atomic_write_data_file(database, table, bytes_data)
            file_size = len(bytes_data)

            change_entry = {
                'record_id': rec_id,
                'operation': op,
                'before': before,
                'after': after,
                'processed_at': datetime.now().isoformat()
            }
            change_log_path = self.write_change_log(database, table, [change_entry])

            op_summary = {
                'batch_size': 1,
                'inserts': 1 if op == 'INSERT' else 0,
                'updates': 1 if op == 'UPDATE' else 0,
                'deletes': 1 if op == 'DELETE' else 0,
                'total_records': len(df_combined),
                'file_size': file_size,
                'primary_operation': op.lower(),
                'duration_ms': 0
            }
            manifest_path = self.create_manifest_file(database, table, data_key, len(df_combined), file_size, op_summary)
            manifest_list_path = self.create_manifest_list(database, table, [manifest_path], len(df_combined))

            change_type = 'schema_change' if schema_changed else 'stream'
            self.create_snapshot(metadata, manifest_list_path, op_summary, change_log_path=change_log_path, change_type=change_type)
            self.save_metadata(database, table, metadata)

            logger.info(f"Processed stream op={op} for {table_key} rec={rec_id} (schema_changed={schema_changed})")

        except Exception as e:
            err = {"table": table_key, "record_id": record.get('_record_id'), "error": str(e), "time": datetime.now().isoformat()}
            logger.exception(f"Stream processing error for {table_key}: {e}")
            self.write_error_log(database, table, [err])
            errors.append(err)
        return errors
    

    # =========================
    # DDL processing
    # =========================
    def process_ddl_event(self, payload):
        op = payload.get("operation", "").upper()
        raw_db = payload.get("database")
        raw_table = payload.get("table")
        db, table = self.normalize_db_table(raw_db, raw_table)
        logger.info(f"Processing DDL event: {op} on {raw_db}.{raw_table} -> normalized as {db}.{table}")

        if not db:
            logger.warning("DDL payload missing database")
            return

        if op == "ADD_TABLE":
            if not table:
                logger.warning("Missing table name in ADD_TABLE")
                return
            df_empty = pd.DataFrame()
            arrow_schema = self.df_to_arrow_schema(df_empty)
            metadata = self.create_new_table_metadata(db, table)
            self.add_schema_to_metadata(metadata, arrow_schema)
            self.save_metadata(db, table, metadata)
            logger.info(f"✓ Created empty table {db}.{table}")
            return

        if op == "DROP_TABLE":
            if not table:
                return
            new_table = f"{table}_deleted"
            logger.info(f"Renaming dropped table {table} → {new_table}")
            self._rename_table_in_s3(db, table, new_table)
            return

        if op == "RENAME_TABLE":
            old = payload.get("old_table")
            new = payload.get("new_table")
            _, old_table = self.normalize_db_table(db, old) if old else (db, None)
            _, new_table = self.normalize_db_table(db, new) if new else (db, None)
            if not old_table or not new_table:
                return
            logger.info(f"Renaming table {old_table} → {new_table}")
            self._rename_table_in_s3(db, old_table, new_table)
            return

        if op == "ADD_COLUMN":
            col = payload.get("column")
            default = payload.get("default", "")
            if not col:
                logger.warning("ADD_COLUMN missing column name")
                return

            existing_table = self.read_existing_table(db, table)
            if existing_table is None:
                logger.info(f"Table {db}.{table} not found for ADD_COLUMN — creating metadata and empty data file with column {col}")
                metadata = self.load_table_metadata(db, table)  
                df = pd.DataFrame(columns=[col])
                if default is not None and default != "":
                    df[col] = default
                if 'deleted' not in df.columns:
                    df['deleted'] = False
                self._write_back_table_with_schema(db, table, df)
                logger.info(f"✓ Added column {col} (default={default}) to newly created {db}.{table}")
                return

            df = existing_table.to_pandas().astype(str)
            if col not in df.columns:
                df[col] = default if default is not None else ''
            self._write_back_table_with_schema(db, table, df)
            logger.info(f"✓ Added column {col} (default={default}) to {db}.{table}")
            return

        if op == "RENAME_COLUMN":
            old_col = payload.get("old_column")
            new_col = payload.get("new_column")
            if not old_col or not new_col:
                logger.warning("RENAME_COLUMN missing old/new column")
                return
            existing_table = self.read_existing_table(db, table)
            if existing_table is None:
                logger.warning(f"Table {db}.{table} not found for RENAME_COLUMN")
                return
            df = existing_table.to_pandas().astype(str)
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
                self._write_back_table_with_schema(db, table, df)
                logger.info(f"✓ Renamed column {old_col} → {new_col} in {db}.{table}")
            return

        if op == "DROP_COLUMN":
            col = payload.get("column")
            if not col:
                logger.warning("DROP_COLUMN missing column")
                return
            existing_table = self.read_existing_table(db, table)
            if existing_table is None:
                logger.warning(f"Table {db}.{table} not found for DROP_COLUMN")
                return
            df = existing_table.to_pandas().astype(str)
            if col in df.columns:
                new_col = f"{col}_deleted"
                df.rename(columns={col: new_col}, inplace=True)
                self._write_back_table_with_schema(db, table, df)
                logger.info(f"✓ Renamed column {col} → {new_col} (DROP_COLUMN) in {db}.{table}")
            return

        logger.info(f"✅ Finished DDL: {op} on {db}.{table}")


    
    # =========================
    # Kafka consumer
    # =========================
    def connect_kafka(self):
        try:
            self.consumer = KafkaConsumer(
                bootstrap_servers=self.kafka_servers.split(","),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                group_id="iceberg-batch-processor-group"
            )
            self.consumer.subscribe(pattern=".*")  
            logger.info("✓ Connected to Kafka, subscribed to all topics")
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")
            raise

    # =========================
    # Main processing loop
    # =========================
    def process(self):
        try:
            self.wait_for_services()
        except Exception:
            logger.warning("Service wait failed or skipped")

        self.connect_kafka()
        logger.info("🚀 Processor started (init-batch + stream-per-record)")

        init_buffers = defaultdict(list)
        
        for message in self.consumer:
            try:
                self.record_count += 1
                record = message.value

                logger.info(f"[{self.record_count}] Topic: {message.topic}")
                logger.info(f"[{self.record_count}] Raw payload: {json.dumps(record, ensure_ascii=False)}")

                op = str(record.get("operation", "")).upper()
                if op in ("ADD_TABLE", "DROP_TABLE", "RENAME_TABLE",
                        "ADD_COLUMN", "DROP_COLUMN", "RENAME_COLUMN"):
                    self.process_ddl_event(record)
                    continue

                transformed, table_name, database_name, operation = self.transform_record(record)
                if not transformed or not table_name or not database_name:
                    logger.warning(f"[{self.record_count}] Failed to transform record")
                    continue

                table_key = f"{database_name}.{table_name}"

                if table_key not in self.init_loaded_tables:
                    if self.read_existing_table(database_name, table_name) is not None:
                        self.init_loaded_tables.add(table_key)
                        logger.info(f"Detected existing data for {table_key}, switching to stream mode")
                    else:
                        init_buffers[table_key].append(transformed)
                        logger.info(f"[{self.record_count}] Buffered for INIT: {table_key} | {len(init_buffers[table_key])} records")

                        if len(init_buffers[table_key]) >= self.init_batch_size:
                            total = self.process_init_batches(table_key, init_buffers[table_key])
                            logger.info(f"Init load completed chunk for {table_key} -> {total} records written")
                            init_buffers[table_key] = []
                        continue  

                if operation in ("INSERT", "UPDATE", "DELETE"):
                    self.process_stream_record(table_key, transformed)

            except Exception as e:
                logger.exception(f"Processing loop error: {e}")



    def wait_for_services(self):
        logger.info("Waiting for Kafka...")
        try:
            host, port = self.kafka_servers.split(":")
            for _ in range(10):
                try:
                    s = socket.create_connection((host, int(port)), timeout=2)
                    s.close()
                    logger.info("✓ Kafka ready")
                    break
                except Exception:
                    time.sleep(1)
        except Exception:
            logger.debug("Skipping Kafka wait")

        logger.info("Waiting for Flink JobManager... (best-effort)")
        try:
            for _ in range(5):
                try:
                    r = requests.get(f"http://{self.flink_host}:{self.flink_port}/overview", timeout=2)
                    if r.status_code == 200:
                        logger.info("✓ Flink ready")
                        return
                except:
                    time.sleep(1)
        except Exception:
            logger.debug("Skipping Flink wait")


if __name__ == "__main__":
    try:
        processor = IcebergBatchProcessor()
        processor.process()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    except Exception as e:
        logger.exception(f"Fatal: {e}")
        raise
