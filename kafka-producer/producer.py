import os
import time
import json
import logging
import threading
import hashlib
import psycopg2
import psycopg2.extensions
import pymssql
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class UnifiedKafkaProducer:
    def __init__(self):
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = None
        
        self.enable_postgres = os.getenv('ENABLE_POSTGRES', 'true').lower() == 'true'
        self.pg_config = {
            'host': os.getenv('PG_HOST', 'host.docker.internal'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'database': os.getenv('PG_DATABASE', 'MDI'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', '12345'),
            'connect_timeout': 10
        }
        self.pg_connection = None
        self.pg_table_snapshots = {}        
        self.pg_table_schemas = {}          
        self.pg_table_schema_versions = {}
        self.pg_schema_changed = {}       

        self.enable_mssql = os.getenv('ENABLE_MSSQL', 'true').lower() == 'true'
        self.mssql_config = {
            'host': os.getenv('MSSQL_HOST', 'host.docker.internal'),
            'port': int(os.getenv('MSSQL_PORT', 1433)),
            'database': os.getenv('MSSQL_DATABASE', 'Mpass'),
            'user': os.getenv('MSSQL_USER', 'mssql'),
            'password': os.getenv('MSSQL_PASSWORD', '12345'),
            'timeout': 30,
            'login_timeout': 10
        }
        self.mssql_connection = None
        self.mssql_table_snapshots = {}
        self.mssql_table_schemas = {}
        self.mssql_table_schema_versions = {}
        self.mssql_schema_changed = {}

        self.mssql_poll_interval = int(os.getenv('MSSQL_POLL_INTERVAL', 5))

        self.message_count = 0
        self.lock = threading.Lock()

    def connect_kafka(self, max_retries=5):
        if self.producer:
            return
        attempt, backoff = 0, 1
        while attempt < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers.split(','),
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    acks='all',
                    retries=5,
                    max_in_flight_requests_per_connection=1
                )
                return
            except Exception as e:
                attempt += 1
                logger.warning(f"Kafka connection failed ({attempt}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise Exception("Failed to connect to Kafka")


    def _apply_ddl_to_snapshots(self, payload):
        op = payload.get("operation")
        src = payload.get("source", "postgresql").lower()
        db = payload.get("database")

        if src == "postgresql":
            table_schemas = self.pg_table_schemas
            table_snapshots = self.pg_table_snapshots
            schema_versions = self.pg_table_schema_versions
        elif src == "mssql":
            table_schemas = self.mssql_table_schemas
            table_snapshots = self.mssql_table_snapshots
            schema_versions = self.mssql_table_schema_versions
        else:
            return

        with self.lock:
            try:
                if op == "ADD_TABLE":
                    full_table = payload['table']
                    table_schemas.setdefault(full_table, payload.get("columns", {}))
                    table_snapshots.setdefault(full_table, {})
                    schema_versions[full_table] = schema_versions.get(full_table, 0) + 1

                elif op == "DROP_TABLE":
                    full_table = payload['table']
                    table_schemas.pop(full_table, None)
                    table_snapshots.pop(full_table, None)
                    schema_versions.pop(full_table, None)

                elif op == "RENAME_TABLE":
                    old = payload['old_table']
                    new = payload['new_table']
                    if old in table_schemas:
                        table_schemas[new] = table_schemas.pop(old)
                    if old in table_snapshots:
                        table_snapshots[new] = table_snapshots.pop(old)
                    if old in schema_versions:
                        schema_versions[new] = schema_versions.pop(old)

                elif op == "ADD_COLUMN":
                    full_table = payload['table']
                    col = payload['column']
                    dtype = payload.get('data_type')
                    default = payload.get('default', None)
                    cols = table_schemas.setdefault(full_table, {})
                    cols[col] = dtype
                    schema_versions[full_table] = schema_versions.get(full_table, 0) + 1

                    snaps = table_snapshots.get(full_table, {})
                    for rid, row in snaps.items():
                        row.setdefault(col, default)

                elif op == "DROP_COLUMN":
                    full_table = payload['table']
                    col = payload['column']
                    cols = table_schemas.get(full_table, {})
                    if col in cols:
                        cols.pop(col, None)
                        schema_versions[full_table] = schema_versions.get(full_table, 0) + 1
                    snaps = table_snapshots.get(full_table, {})
                    for rid, row in snaps.items():
                        if col in row:
                            row.pop(col, None)

                elif op == "RENAME_COLUMN":
                    full_table = payload['table']
                    old_col = payload['old_column']
                    new_col = payload['new_column']
                    cols = table_schemas.get(full_table, {})
                    if old_col in cols:
                        dtype = cols.pop(old_col)
                        cols[new_col] = dtype
                        schema_versions[full_table] = schema_versions.get(full_table, 0) + 1

                    snaps = table_snapshots.get(full_table, {})
                    for rid, row in snaps.items():
                        if old_col in row:
                            row[new_col] = row.pop(old_col)
                        else:
                            row.setdefault(new_col, None)

                return
            except Exception:
                logger.exception("Error applying DDL to snapshots")
                return



    def _send_ddl_payload(self, payload):
        source = payload.get("source", "postgresql").lower()

        if source == "postgresql":
            topic = f"{self.pg_config['database']}-ddl-topic"
        elif source == "mssql":
            topic = f"{self.mssql_config['database']}-ddl-topic"
        else:
            topic = "unknown-ddl-topic"

        try:
            self.producer.send(topic, value=payload)
            logger.info(f"[DDL]: {payload}")
        except Exception:
            logger.exception(f"Failed to send {source.upper()} DDL payload to Kafka")
        finally:
            try:
                self._apply_ddl_to_snapshots(payload)
            except Exception:
                logger.exception(f"Failed to apply {source.upper()} DDL to local snapshots")


    def connect_postgres(self):
        if not self.enable_postgres:
            return
        self.pg_connection = psycopg2.connect(**self.pg_config)
        self.pg_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def postgres_initial_load(self):
        """Initial load for all PostgreSQL tables: populate snapshots and schemas"""
        if not self.enable_postgres:
            return {}

        try:
            if not self.pg_connection:
                self.connect_postgres()
            cursor = self.pg_connection.cursor()
            cursor.execute("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE table_type='BASE TABLE'
                AND table_schema NOT IN ('pg_catalog', 'information_schema');
            """)
            tables = [f"{r[0]}.{r[1]}" for r in cursor.fetchall()]

            all_snapshots = {}

            for table_full in tables:
                schema, table = table_full.split(".")
                topic = f"{self.pg_config['database']}-{table}-topic"
                logger.info(f"[INITLOAD]: {table_full}")

                cursor.execute("""
                    SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (schema, table))
                cols = cursor.fetchall()
                col_map = {c[0]: c[1] for c in cols}
                self.pg_table_schemas[table_full] = col_map
                self.pg_table_schema_versions[table_full] = 0
                self.pg_schema_changed[table_full] = False

                cursor.execute(f'SELECT * FROM "{schema}"."{table}";')
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                total = len(rows)
                logger.info(f"{total} rows in {table_full}")

                snapshot = {}
                for idx, row in enumerate(rows, start=1):
                    record = dict(zip(columns, row))
                    record_id = record.get("stt") or hashlib.md5(json.dumps(record, sort_keys=True).encode()).hexdigest()
                    snapshot[record_id] = record

                    payload = {
                        "operation": "INITIAL_LOAD",
                        "table": table,
                        "database": self.pg_config['database'],
                        "source": "postgresql",
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "data": record
                    }
                    self.producer.send(topic, value=payload)

                self.producer.flush()
                logger.info(f"load completed for {table_full} ({total} rows)")
                all_snapshots[table_full] = snapshot
                self.pg_table_snapshots[table_full] = snapshot

            cursor.close()
            return all_snapshots
        except Exception:
            logger.exception("PostgreSQL initial load error")
            return {}


    def postgres_poll_rows(self):
        if not self.enable_postgres:
            return

        poll_interval = int(os.getenv('PG_POLL_INTERVAL', 10))

        while True:
            try:
                if not self.pg_connection or self.pg_connection.closed:
                    self.connect_postgres()
                cursor = self.pg_connection.cursor()

                cursor.execute("""
                    SELECT table_schema, table_name
                    FROM information_schema.tables
                    WHERE table_type='BASE TABLE'
                    AND table_schema NOT IN ('pg_catalog', 'information_schema');
                """)
                tables = [f"{r[0]}.{r[1]}" for r in cursor.fetchall()]

                for table_full in tables:
                    schema, table = table_full.split(".")
                    topic = f"{self.pg_config['database']}-{table}-topic"

                    cursor.execute(f'SELECT * FROM "{schema}"."{table}";')
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()

                    last_snapshot = self.pg_table_snapshots.get(table_full, {})
                    current_snapshot = {}

                    for row in rows:
                        row_dict = dict(zip(columns, row))
                        row_id = row_dict.get("stt") or hashlib.md5(json.dumps(row_dict, sort_keys=True).encode()).hexdigest()
                        current_snapshot[row_id] = row_dict

                        old_row = last_snapshot.get(row_id)
                        if not old_row:
                            op = "INSERT"
                        if old_row is None:
                            op = "INSERT"
                        else:
                            common_cols = set(old_row.keys()) & set(row_dict.keys())
                            changed = any(old_row.get(c) != row_dict.get(c) for c in common_cols)
                            if changed:
                                op = "UPDATE"
                            else:
                                continue

                        payload = {
                            "operation": op,
                            "table": table,
                            "database": self.pg_config["database"],
                            "source": "postgresql",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "data": row_dict
                        }
                        self.producer.send(topic, value=payload)
                        with self.lock:
                            self.message_count += 1
                            logger.info(f"[{op}]: {payload}")

                    deleted_ids = set(last_snapshot.keys()) - set(current_snapshot.keys())
                    for record_id in deleted_ids:
                        payload = {
                            "operation": "DELETE",
                            "table": table,
                            "database": self.pg_config["database"],
                            "source": "postgresql",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "data": {"stt": record_id},
                        }
                        self.producer.send(topic, value=payload)
                        with self.lock:
                            self.message_count += 1
                            logger.info(f"DELETE: {payload}")

                    self.pg_table_snapshots[table_full] = current_snapshot

                cursor.close()
                time.sleep(poll_interval)

            except Exception:
                logger.exception("Error polling PostgreSQL ROWS")
                time.sleep(5)



    def load_pg_schema_state(self):
        if not self.pg_connection or self.pg_connection.closed:
            self.connect_postgres()

        schema_state = {}
        cursor = self.pg_connection.cursor()
        cursor.execute("""
            SELECT table_schema, table_name, column_name, data_type, column_default
            FROM information_schema.columns
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
            ORDER BY table_schema, table_name, ordinal_position;
        """)
        rows = cursor.fetchall()

        for row in rows:
            schema, table, col, dtype, default = row
            full_table = f"{schema}.{table}"
            if full_table not in schema_state:
                schema_state[full_table] = {}
            schema_state[full_table][col] = {
                "data_type": dtype,
                "column_default": default
            }

        cursor.close()
        return schema_state


    def postgres_poll_ddl(self):

        ddl_state = self.load_pg_schema_state()  

        while True:
            try:
                time.sleep(5)
                new_state = self.load_pg_schema_state()

                old_tables = set(ddl_state.keys())
                new_tables = set(new_state.keys())

                added_tables = new_tables - old_tables
                for tbl in added_tables:
                    payload = {
                        "operation": "ADD_TABLE",
                        "database": self.pg_config["database"],
                        "table": tbl,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "columns": new_state[tbl],
                        "source": "postgresql"
                    }
                    self._send_ddl_payload(payload)
                    

                dropped_tables = old_tables - new_tables
                for tbl in dropped_tables:
                    payload = {
                        "operation": "DROP_TABLE",
                        "database": self.pg_config["database"],
                        "table": tbl,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "source": "postgresql"
                    }
                    self._send_ddl_payload(payload)

                for dropped in list(dropped_tables):
                    for added in list(added_tables):
                        if new_state[added] == ddl_state[dropped]:
                            payload = {
                                "operation": "RENAME_TABLE",
                                "database": self.pg_config["database"],
                                "old_table": dropped,
                                "new_table": added,
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                "source": "postgresql"
                            }
                            self._send_ddl_payload(payload)
                            dropped_tables.discard(dropped)
                            added_tables.discard(added)
                            break
                    

                for tbl in old_tables & new_tables:
                    old_cols = ddl_state[tbl]
                    new_cols = new_state[tbl]

                    added_cols = set(new_cols.keys()) - set(old_cols.keys())
                    dropped_cols = set(old_cols.keys()) - set(new_cols.keys())

                    rename_pairs = []
                    for dropped in list(dropped_cols):
                        for added in list(added_cols):
                            if old_cols[dropped]["data_type"] == new_cols[added]["data_type"]:
                                old_def = old_cols[dropped].get("column_default")
                                new_def = new_cols[added].get("column_default")
                                if old_def == new_def or (old_def is None and new_def is None):
                                    rename_pairs.append((dropped, added))
                                    dropped_cols.discard(dropped)
                                    added_cols.discard(added)
                                    break

                    for old_col, new_col in rename_pairs:
                        payload = {
                            "operation": "RENAME_COLUMN",
                            "database": self.pg_config["database"],
                            "table": tbl,
                            "old_column": old_col,
                            "new_column": new_col,
                            "data_type": new_cols[new_col]["data_type"],
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "postgresql"
                        }
                        self._send_ddl_payload(payload)

                    for col in added_cols:
                        col_def = new_cols[col]
                        payload = {
                            "operation": "ADD_COLUMN",
                            "database": self.pg_config["database"],
                            "table": tbl,
                            "column": col,
                            "data_type": col_def["data_type"],
                            "default": col_def.get("column_default"),
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "postgresql"
                        }
                        self._send_ddl_payload(payload)

                    for col in dropped_cols:
                        payload = {
                            "operation": "DROP_COLUMN",
                            "database": self.pg_config["database"],
                            "table": tbl,
                            "column": col,
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "postgresql"
                        }
                        self._send_ddl_payload(payload)

                ddl_state = new_state 


            except Exception as e:
                logger.error(f"Error polling PostgreSQL DDL: {e}")
                time.sleep(5)


    def connect_mssql(self):
        if not self.enable_mssql:
            return
        self.mssql_connection = pymssql.connect(
            server=self.mssql_config['host'],
            port=self.mssql_config['port'],
            user=self.mssql_config['user'],
            password=self.mssql_config['password'],
            database=self.mssql_config['database'],
            timeout=self.mssql_config['timeout'],
            login_timeout=self.mssql_config['login_timeout']
        )
    def _open_mssql_conn_with_retry(self, retries=3, backoff=1):
        attempt = 0
        while attempt < retries:
            try:
                conn = pymssql.connect(
                    server=self.mssql_config['host'],
                    port=self.mssql_config['port'],
                    user=self.mssql_config['user'],
                    password=self.mssql_config['password'],
                    database=self.mssql_config['database'],
                    timeout=self.mssql_config['timeout'],
                    login_timeout=self.mssql_config['login_timeout']
                )
                return conn
            except Exception as e:
                attempt += 1
                logger.warning(f"MSSQL connect attempt {attempt}/{retries} failed: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise

    def mssql_initial_load(self):
        if not self.enable_mssql:
            return {}

        try:
            if not self.mssql_connection:
                self.connect_mssql()
            cursor = self.mssql_connection.cursor(as_dict=True)
            cursor.execute("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE='BASE TABLE'
                AND TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA');
            """)
            tables = [f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}" for r in cursor.fetchall()]

            all_snapshots = {}

            for table_full in tables:
                schema, table = table_full.split(".")
                topic = f"{self.mssql_config['database']}-{table}-topic"
                logger.info(f"[INITLOAD]: {table_full}")

                cursor.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s
                    ORDER BY ORDINAL_POSITION
                """, (schema, table))
                cols = cursor.fetchall()
                col_map = {c['COLUMN_NAME']: c['DATA_TYPE'] for c in cols}
                self.mssql_table_schemas[table_full] = col_map
                self.mssql_table_schema_versions[table_full] = 0
                self.mssql_schema_changed[table_full] = False

                cursor.execute(f"SELECT * FROM [{schema}].[{table}];")
                rows = cursor.fetchall()
                total = len(rows)
                logger.info(f"{total} rows in {table_full}")

                snapshot = {}
                for idx, row in enumerate(rows, start=1):
                    record_id = row.get(os.getenv("MSSQL_PRIMARY_KEY", "stt")) or hashlib.md5(
                        json.dumps(row, sort_keys=True).encode()
                    ).hexdigest()
                    snapshot[record_id] = row

                    payload = {
                        "operation": "INITIAL_LOAD",
                        "table": table,
                        "database": self.mssql_config['database'],
                        "source": "mssql",
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "data": row
                    }
                    self.producer.send(topic, value=payload)

                self.producer.flush()
                logger.info(f"load completed for {table_full} ({total} rows)")
                all_snapshots[table_full] = snapshot
                self.mssql_table_snapshots[table_full] = snapshot

            cursor.close()
            return all_snapshots

        except Exception:
            logger.exception("MSSQL initial load error")
            return {}

        
    def mssql_poll_rows(self):
        if not self.enable_mssql:
            return
        
        poll_interval = int(os.getenv('MSSQL_POLL_INTERVAL', 10))

        while True:
            try:
                if not self.mssql_connection:
                    self.connect_mssql()
                cursor = self.mssql_connection.cursor(as_dict=True)

                cursor.execute("""
                    SELECT TABLE_SCHEMA, TABLE_NAME
                    FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_TYPE = 'BASE TABLE'
                    AND TABLE_SCHEMA NOT IN ('sys');
                """)
                tables = [f"{r['TABLE_SCHEMA']}.{r['TABLE_NAME']}" for r in cursor.fetchall()]

                for table_full in tables:
                    schema, table = table_full.split(".")
                    topic = f"{self.mssql_config['database']}-{table}-topic"

                    cursor.execute(f'SELECT * FROM [{schema}].[{table}]')
                    rows = cursor.fetchall()
                    columns = rows[0].keys() if rows else []

                    last_snapshot = self.mssql_table_snapshots.get(table_full, {})
                    current_snapshot = {}

                    for row in rows:
                        row_id = row.get("stt") or hashlib.md5(json.dumps(row, sort_keys=True).encode()).hexdigest()
                        current_snapshot[row_id] = row
                        old_row = last_snapshot.get(row_id)

                        if not old_row:
                            op = "INSERT"
                        else:
                            common_cols = set(old_row.keys()) & set(row.keys())
                            changed = any(old_row.get(c) != row.get(c) for c in common_cols)
                            if changed:
                                op = "UPDATE"
                            else:
                                continue


                        payload = {
                            "operation": op,
                            "table": table,
                            "database": self.mssql_config["database"],
                            "source": "mssql",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "data": row,
                        }
                        self.producer.send(topic, value=payload)
                        with self.lock:
                            self.message_count += 1
                            logger.info(f"[{op}]: {payload}")

                    deleted_ids = set(last_snapshot.keys()) - set(current_snapshot.keys())
                    for record_id in deleted_ids:
                        payload = {
                            "operation": "DELETE",
                            "table": table,
                            "database": self.mssql_config["database"],
                            "source": "mssql",
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "data": {"stt": record_id},
                        }
                        self.producer.send(topic, value=payload)
                        with self.lock:
                            self.message_count += 1
                            logger.info(f"DELETE: {payload}")

                    self.mssql_table_snapshots[table_full] = current_snapshot

                cursor.close()
                time.sleep(poll_interval)

            except Exception:
                logger.exception("Error polling MSSQL ROWS")
                time.sleep(5)

    def load_mssql_schema_state(self):
        conn = self._open_mssql_conn_with_retry()
        cursor = conn.cursor(as_dict=True)
        cursor.execute("""
            SELECT 
                TABLE_SCHEMA AS table_schema,
                TABLE_NAME AS table_name,
                COLUMN_NAME AS column_name,
                DATA_TYPE AS data_type,
                COLUMN_DEFAULT AS column_default
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA NOT IN ('sys', 'INFORMATION_SCHEMA')
            ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
        """)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        schema_state = {}
        for r in rows:
            schema = r["table_schema"]
            table = r["table_name"]
            col = r["column_name"]
            dtype = r["data_type"]
            default = r["column_default"]
            full_table = f"{schema}.{table}"

            if full_table not in schema_state:
                schema_state[full_table] = {}
            schema_state[full_table][col] = {
                "data_type": dtype,
                "column_default": default
            }

        return schema_state


    def mssql_poll_ddl(self):
        if not self.enable_mssql:
            return

        ddl_state = self.load_mssql_schema_state() 
        poll_interval = int(os.getenv("MSSQL_DDL_POLL_INTERVAL", 10))

        while True:
            try:
                time.sleep(poll_interval)
                new_state = self.load_mssql_schema_state()

                old_tables = set(ddl_state.keys())
                new_tables = set(new_state.keys())

                added_tables = new_tables - old_tables
                for tbl in added_tables:
                    payload = {
                        "operation": "ADD_TABLE",
                        "database": self.mssql_config["database"],
                        "table": tbl,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "columns": new_state[tbl],
                        "source": "mssql"
                    }
                    self._send_ddl_payload(payload)

                dropped_tables = old_tables - new_tables
                for tbl in dropped_tables:
                    payload = {
                        "operation": "DROP_TABLE",
                        "database": self.mssql_config["database"],
                        "table": tbl,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "source": "mssql"
                    }
                    self._send_ddl_payload(payload)

                for dropped in list(dropped_tables):
                    for added in list(added_tables):
                        if new_state[added] == ddl_state[dropped]:
                            payload = {
                                "operation": "RENAME_TABLE",
                                "database": self.mssql_config["database"],
                                "old_table": dropped,
                                "new_table": added,
                                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                                "source": "mssql"
                            }
                            self._send_ddl_payload(payload)
                            dropped_tables.discard(dropped)
                            added_tables.discard(added)
                            break

                for tbl in old_tables & new_tables:
                    old_cols = ddl_state[tbl]
                    new_cols = new_state[tbl]

                    added_cols = set(new_cols.keys()) - set(old_cols.keys())
                    dropped_cols = set(old_cols.keys()) - set(new_cols.keys())

                    rename_pairs = []
                    for dropped in list(dropped_cols):
                        for added in list(added_cols):
                            if old_cols[dropped]["data_type"] == new_cols[added]["data_type"]:
                                rename_pairs.append((dropped, added))
                                dropped_cols.discard(dropped)
                                added_cols.discard(added)
                                break

                    for old_col, new_col in rename_pairs:
                        payload = {
                            "operation": "RENAME_COLUMN",
                            "database": self.mssql_config["database"],
                            "table": tbl,
                            "old_column": old_col,
                            "new_column": new_col,
                            "data_type": new_cols[new_col]["data_type"],
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "mssql"
                        }
                        self._send_ddl_payload(payload)

                    for col in added_cols:
                        payload = {
                            "operation": "ADD_COLUMN",
                            "database": self.mssql_config["database"],
                            "table": tbl,
                            "column": col,
                            "data_type": new_cols[col]["data_type"],
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "mssql"
                        }
                        self._send_ddl_payload(payload)

                    for col in dropped_cols:
                        payload = {
                            "operation": "DROP_COLUMN",
                            "database": self.mssql_config["database"],
                            "table": tbl,
                            "column": col,
                            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                            "source": "mssql"
                        }
                        self._send_ddl_payload(payload)

                ddl_state = new_state

            except Exception:
                logger.exception("Error polling MSSQL DDL")
                time.sleep(5)

    
    def shutdown(self):
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
        except:
            pass
        try:
            if self.pg_connection:
                self.pg_connection.close()
        except:
            pass
        try:
            if self.mssql_connection:
                self.mssql_connection.close()
        except:
            pass

    def start(self):
        try:
            self.connect_kafka()

            threads = []
            if self.enable_postgres:
                self.connect_postgres()
                initial_snapshots = self.postgres_initial_load()
                if isinstance(initial_snapshots, dict):
                    for table_full, snapshot in initial_snapshots.items():
                        self.pg_table_snapshots[table_full] = snapshot

                t1 = threading.Thread(target=self.postgres_poll_rows, daemon=True)
                t1.start()
                threads.append(t1)

                
            if self.enable_mssql:
                self.connect_mssql()
                initial_snapshots = self.mssql_initial_load()
                if isinstance(initial_snapshots, dict):
                    for table_full, snapshot in initial_snapshots.items():
                        self.mssql_table_snapshots[table_full] = snapshot

                t3 = threading.Thread(target=self.mssql_poll_rows, daemon=True)
                t3.start()
                threads.append(t3)

            
            if self.enable_postgres:
                t2 = threading.Thread(target=self.postgres_poll_ddl, daemon=True)
                t2.start()
                threads.append(t2)


            if self.enable_mssql:
                t4 = threading.Thread(target=self.mssql_poll_ddl, daemon=True)
                t4.start()
                threads.append(t4)


            for t in threads:
                t.join()

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception:
            logger.exception("Fatal error")
            raise
        finally:
            self.shutdown()


if __name__ == "__main__":
    producer = UnifiedKafkaProducer()
    producer.start()
