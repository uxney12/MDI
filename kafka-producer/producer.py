#!/usr/bin/env python3
import os
import time
import json
import logging
import select
import threading
from typing import Optional

import psycopg2
import psycopg2.extensions
import pymssql
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UnifiedKafkaProducer:
    def __init__(self):
        # --- Cấu hình Kafka ---
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = None
        
        # --- Cấu hình PostgreSQL ---
        self.enable_postgres = os.getenv('ENABLE_POSTGRES', 'true').lower() == 'true'
        self.pg_config = {
            'host': os.getenv('PG_HOST', 'host.docker.internal'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'database': os.getenv('PG_DATABASE', 'MDI'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', '12345'),
            'connect_timeout': 10,
            'keepalives': 1,
            'keepalives_idle': 30,
            'keepalives_interval': 10,
            'keepalives_count': 5,
        }
        self.pg_connection = None
        self.pg_table = 'mcc'
        self.pg_topic = 'postgres-mcc-changes'
        
        # --- Cấu hình MS SQL Server ---
        self.enable_mssql = os.getenv('ENABLE_MSSQL', 'true').lower() == 'true'
        self.mssql_config = {
            'host': os.getenv('MSSQL_HOST', 'host.docker.internal'),
            'port': int(os.getenv('MSSQL_PORT', 1433)),
            'database': os.getenv('MSSQL_DATABASE', 'Mpass'),
            'user': os.getenv('MSSQL_USER', 'mssql'),
            'password': os.getenv('MSSQL_PASSWORD', '12345'),
            'timeout': 30,
            'login_timeout': 10,
        }
        self.mssql_connection = None
        self.mssql_table = 'mpass'
        self.mssql_topic = 'mssql-mpass-changes'
        self.mssql_poll_interval = int(os.getenv('MSSQL_POLL_INTERVAL', 5))
        
        # Message counter
        self.message_count = 0
        self.lock = threading.Lock()
        
        logger.info("=== Unified Kafka Producer Configuration ===")
        logger.info(f"Kafka Servers: {self.kafka_servers}")
        if self.enable_postgres:
            logger.info(f"PostgreSQL: ENABLED - {self.pg_config['database']}.{self.pg_table}")
        else:
            logger.info("PostgreSQL: DISABLED")
        if self.enable_mssql:
            logger.info(f"MS SQL Server: ENABLED - {self.mssql_config['database']}.{self.mssql_table}")
        else:
            logger.info("MS SQL Server: DISABLED")

    # ===================== KAFKA ======================
    def connect_kafka(self, max_retries=5):
        if self.producer:
            return
        attempt = 0
        backoff = 1
        while attempt < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.kafka_servers.split(','),
                    value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
                    acks='all',
                    retries=5,
                    max_in_flight_requests_per_connection=1
                )
                logger.info(f"✓ Connected to Kafka: {self.kafka_servers}")
                return
            except Exception as e:
                attempt += 1
                logger.warning(f"Kafka connection failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise Exception("Failed to connect to Kafka")

    # ===================== POSTGRESQL ======================
    def connect_postgres(self, max_retries=30):
        if not self.enable_postgres:
            return
        attempt = 0
        backoff = 1
        while attempt < max_retries:
            try:
                self.pg_connection = psycopg2.connect(**self.pg_config)
                self.pg_connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                logger.info(f"✓ Connected to PostgreSQL: {self.pg_config['database']}")
                return
            except Exception as e:
                attempt += 1
                logger.warning(f"PostgreSQL connection failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise Exception("Failed to connect to PostgreSQL")

    def postgres_initial_load(self):
        if not self.enable_postgres or not self.pg_connection:
            return
        try:
            cursor = self.pg_connection.cursor()
            logger.info(f"🚀 Starting initial load: PostgreSQL.{self.pg_table}")
            cursor.execute(f"SELECT * FROM {self.pg_table};")
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            total = len(rows)
            logger.info(f"Found {total} rows in {self.pg_table}")
            
            for idx, row in enumerate(rows, start=1):
                payload = {
                    "operation": "INITIAL_LOAD",
                    "table": self.pg_table,
                    "database": self.pg_config['database'],
                    "source": "postgresql",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "data": dict(zip(columns, row))
                }
                self.producer.send(self.pg_topic, value=payload)
                if idx % 500 == 0:
                    logger.info(f"PostgreSQL: Sent {idx}/{total} rows")
                    self.producer.flush()
            
            self.producer.flush()
            logger.info(f"✓ PostgreSQL initial load completed ({total} rows)")
            cursor.close()
        except Exception as e:
            logger.exception("❌ PostgreSQL initial load error")

    def postgres_listen_loop(self):
        if not self.enable_postgres:
            return
        
        backoff = 1
        logger.info(f"Starting PostgreSQL listener thread for {self.pg_table}")
        
        while True:
            try:
                if not self.pg_connection or getattr(self.pg_connection, "closed", 1) != 0:
                    logger.info("PostgreSQL: Reconnecting...")
                    self.connect_postgres()
                
                cursor = self.pg_connection.cursor()
                cursor.execute("UNLISTEN *;")
                cursor.execute("LISTEN mcc_changes;")
                logger.info("✓ PostgreSQL LISTEN registered: mcc_changes")
                
                while True:
                    ready = select.select([self.pg_connection], [], [], 5)
                    if ready[0]:
                        self.pg_connection.poll()
                        while self.pg_connection.notifies:
                            notify = self.pg_connection.notifies.pop(0)
                            try:
                                payload = json.loads(notify.payload)
                                self.producer.send(self.pg_topic, value=payload)
                                
                                with self.lock:
                                    self.message_count += 1
                                    count = self.message_count
                                
                                operation = payload.get('operation', 'UNKNOWN')
                                logger.info(
                                    f"[{count}] ✓ PostgreSQL | "
                                    f"Table: {self.pg_table} | Op: {operation:10} | "
                                    f"Topic: {self.pg_topic}"
                                )
                            except Exception as e:
                                logger.exception("Error processing PostgreSQL notification")
                
                backoff = 1
                
            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                logger.error(f"PostgreSQL connection error: {e}")
                self._close_postgres()
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                logger.exception("Unexpected error in PostgreSQL loop")
                time.sleep(5)

    def _close_postgres(self):
        try:
            if self.pg_connection:
                self.pg_connection.close()
        except:
            pass
        finally:
            self.pg_connection = None

    # ===================== MS SQL SERVER ======================
    def connect_mssql(self, max_retries=30):
        if not self.enable_mssql:
            return
        attempt = 0
        backoff = 1
        while attempt < max_retries:
            try:
                self.mssql_connection = pymssql.connect(
                    server=self.mssql_config['host'],
                    port=self.mssql_config['port'],
                    user=self.mssql_config['user'],
                    password=self.mssql_config['password'],
                    database=self.mssql_config['database'],
                    timeout=self.mssql_config['timeout'],
                    login_timeout=self.mssql_config['login_timeout']
                )
                logger.info(f"✓ Connected to MS SQL: {self.mssql_config['database']}")
                return
            except Exception as e:
                attempt += 1
                logger.warning(f"MS SQL connection failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise Exception("Failed to connect to MS SQL Server")

    def mssql_initial_load(self):
        if not self.enable_mssql or not self.mssql_connection:
            return
        try:
            cursor = self.mssql_connection.cursor(as_dict=True)
            logger.info(f"🚀 Starting initial load: MSSQL.{self.mssql_table}")
            cursor.execute(f"SELECT * FROM {self.mssql_table};")
            rows = cursor.fetchall()
            total = len(rows)
            logger.info(f"Found {total} rows in {self.mssql_table}")
            
            for idx, row in enumerate(rows, start=1):
                payload = {
                    "operation": "INITIAL_LOAD",
                    "table": self.mssql_table,
                    "database": self.mssql_config['database'],
                    "source": "mssql",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                    "data": row
                }
                self.producer.send(self.mssql_topic, value=payload)
                if idx % 500 == 0:
                    logger.info(f"MS SQL: Sent {idx}/{total} rows")
                    self.producer.flush()
            
            self.producer.flush()
            logger.info(f"✓ MS SQL initial load completed ({total} rows)")
            cursor.close()
        except Exception as e:
            logger.exception("❌ MS SQL initial load error")

    def mssql_poll_loop(self):
        if not self.enable_mssql:
            return
        
        backoff = 1
        last_processed_id = 0
        logger.info(f"Starting MS SQL poller thread for {self.mssql_table} (interval: {self.mssql_poll_interval}s)")
        
        while True:
            try:
                if not self.mssql_connection:
                    logger.info("MS SQL: Reconnecting...")
                    self.connect_mssql()

                cursor = self.mssql_connection.cursor(as_dict=True)
                cursor.execute(f"""
                    SELECT TOP 100 id, operation, changed_at, data
                    FROM mpass_audit_log
                    WHERE id > {last_processed_id}
                    ORDER BY id ASC
                """)
                audit_rows = cursor.fetchall()
                
                for row in audit_rows:
                    try:
                        payload = json.loads(row['data'])
                        self.producer.send(self.mssql_topic, value=payload)
                        
                        with self.lock:
                            self.message_count += 1
                            count = self.message_count
                        
                        logger.info(
                            f"[{count}] ✓ MS SQL (audit) | "
                            f"Op: {row['operation']:10} | Topic: {self.mssql_topic}"
                        )
                        last_processed_id = row['id']
                    except Exception as e:
                        logger.exception("Error processing MS SQL audit log")
                
                cursor.close()
                backoff = 1
                time.sleep(self.mssql_poll_interval)
                
            except pymssql.Error as e:
                logger.error(f"MS SQL connection error: {e}")
                self._close_mssql()
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
            except Exception as e:
                logger.exception("Unexpected error in MS SQL loop")
                time.sleep(5)


    def _close_mssql(self):
        try:
            if self.mssql_connection:
                self.mssql_connection.close()
        except:
            pass
        finally:
            self.mssql_connection = None

    # ===================== MAIN ======================
    def shutdown(self):
        logger.info("Shutting down...")
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
        except:
            pass
        self._close_postgres()
        self._close_mssql()
        logger.info("Shutdown complete")

    def start(self):
        try:
            logger.info("🚀 Starting Unified Kafka Producer...")
            
            self.connect_kafka()
            
            if self.enable_postgres:
                self.connect_postgres()
                self.postgres_initial_load()
            
            if self.enable_mssql:
                self.connect_mssql()
                self.mssql_initial_load()
            
            threads = []
            
            if self.enable_postgres:
                pg_thread = threading.Thread(target=self.postgres_listen_loop, daemon=True)
                pg_thread.start()
                threads.append(pg_thread)
            
            if self.enable_mssql:
                mssql_thread = threading.Thread(target=self.mssql_poll_loop, daemon=True)
                mssql_thread.start()
                threads.append(mssql_thread)
            
            logger.info("=" * 60)
            logger.info("✓ All listeners started")
            logger.info("  Press Ctrl+C to stop")
            logger.info("=" * 60)
            
            for thread in threads:
                thread.join()
            
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