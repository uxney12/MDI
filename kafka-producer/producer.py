#!/usr/bin/env python3
import os
import time
import json
import logging
import select
import traceback

import psycopg2
import psycopg2.extensions
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PostgresKafkaProducer:
    def __init__(self):
        # --- cấu hình PostgreSQL ---
        self.pg_config = {
            'host': os.getenv('PG_HOST', 'host.docker.internal'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'database': os.getenv('PG_DATABASE', 'MDI'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASSWORD', '12345'),
            # Kết nối nhanh / keepalive (thông số libpq)
            'connect_timeout': int(os.getenv('PG_CONNECT_TIMEOUT', 10)),
            'keepalives': 1,
            'keepalives_idle': int(os.getenv('PG_KEEPALIVES_IDLE', 30)),
            'keepalives_interval': int(os.getenv('PG_KEEPALIVES_INTERVAL', 10)),
            'keepalives_count': int(os.getenv('PG_KEEPALIVES_COUNT', 5)),
        }

        # --- cấu hình Kafka ---
        self.kafka_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
        self.producer = None
        self.connection = None

        logger.info("=== Kafka Producer Configuration ===")
        logger.info(f"PostgreSQL Host: {self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}")
        logger.info(f"Kafka Servers: {self.kafka_servers}")

    # ---------------------- Kết nối Kafka (với retry) ----------------------
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
        raise Exception("Failed to connect to Kafka after multiple retries")

    # ---------------------- Kết nối PostgreSQL (với keepalives + retry) ----------------------
    def connect_postgres(self, max_retries=30):
        attempt = 0
        backoff = 1
        last_exc = None
        while attempt < max_retries:
            try:
                conn_kwargs = self.pg_config.copy()
                # psycopg2 sẽ chấp nhận các keyword trên (libpq options)
                self.connection = psycopg2.connect(**conn_kwargs)
                # đảm bảo autocommit để LISTEN/NOTIFY hoạt động ổn định
                self.connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                logger.info(f"✓ Connected to PostgreSQL: {self.pg_config['host']}:{self.pg_config['port']}/{self.pg_config['database']}")
                # Test đơn giản
                cur = self.connection.cursor()
                cur.execute("SELECT version();")
                ver = cur.fetchone()
                logger.info(f"PostgreSQL version: {ver[0] if ver else 'unknown'}")
                cur.close()
                return
            except Exception as e:
                attempt += 1
                last_exc = e
                logger.warning(f"PostgreSQL connection failed (attempt {attempt}/{max_retries}): {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)
        raise Exception("Failed to connect to PostgreSQL after multiple retries") from last_exc

    def setup_listen_channels(self, cursor):
        # Hủy listen cũ rồi đăng ký lại
        try:
            cursor.execute("UNLISTEN *;")
        except Exception:
            pass
        cursor.execute("LISTEN mcc_changes;")
        cursor.execute("LISTEN mpass_changes;")
        logger.info("LISTEN registered: mcc_changes, mpass_changes")

    # ---------------------- Nạp dữ liệu lần đầu (giữ nguyên logic) ----------------------
    def initial_load(self):
        if not self.connection:
            logger.warning("initial_load: no DB connection")
            return
        try:
            cursor = self.connection.cursor()
            tables = [
                ("mcc", "mcc-changes"),
                ("mpass", "mpass-changes")
            ]
            for table, topic in tables:
                logger.info(f"🚀 Starting initial load for table: {table}")
                cursor.execute(f"SELECT * FROM {table};")
                columns = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                total = len(rows)
                logger.info(f"Found {total} rows in table '{table}'")
                for idx, row in enumerate(rows, start=1):
                    data = dict(zip(columns, row))
                    payload = {
                        "operation": "INITIAL_LOAD",
                        "table": table.upper(),
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
                        "data": data
                    }
                    # đảm bảo Kafka producer sẵn sàng
                    self.connect_kafka()
                    self.producer.send(topic, value=payload)
                    if idx % 500 == 0:
                        logger.info(f"Sent {idx}/{total} rows for {table}")
                        self.producer.flush()
                self.producer.flush()
                logger.info(f"✓ Completed initial load for table '{table}' ({total} rows)")
            cursor.close()
        except Exception as e:
            logger.exception("❌ Error during initial load")

    # ---------------------- Xử lý notification riêng ----------------------
    def handle_notification(self, notify, message_count):
        try:
            payload = json.loads(notify.payload)
        except json.JSONDecodeError:
            logger.error("Notification payload is not valid JSON: %s", notify.payload)
            return

        table_name = payload.get('table')
        operation = payload.get('operation')

        topic = {
            'MCC': 'mcc-changes',
            'MPASS': 'mpass-changes'
        }.get(table_name)

        if not topic:
            logger.warning(f"Unknown table in payload: {table_name} | raw: {notify.payload}")
            return

        # ensure kafka
        try:
            self.connect_kafka()
        except Exception as e:
            logger.error("Cannot connect to Kafka, skipping message: %s", e)
            return

        try:
            future = self.producer.send(topic, value=payload)
            result = future.get(timeout=10)
            logger.info(
                f"[{message_count}] ✓ Sent to Kafka | Table: {table_name:6} | Operation: {operation:10} | "
                f"Topic: {topic:15} | Partition: {result.partition} | Offset: {result.offset}"
            )
        except KafkaError as e:
            logger.error(f"Kafka send error: {e}")
        except Exception:
            logger.exception("Unexpected error while sending to Kafka")

    # ---------------------- Vòng lắng nghe chính (tự động reconnect) ----------------------
    def listen_and_produce(self):
        message_count = 0
        postgres_backoff = 1

        while True:
            try:
                # đảm bảo Postgres kết nối
                if not self.connection or getattr(self.connection, "closed", 1) != 0:
                    logger.info("DB connection missing or closed -> connecting...")
                    self.connect_postgres()

                cursor = self.connection.cursor()
                self.setup_listen_channels(cursor)

                logger.info("=" * 60)
                logger.info("✓ Listening for PostgreSQL notifications")
                logger.info("  Channels: mcc_changes, mpass_changes")
                logger.info("  Waiting for database changes...")
                logger.info("=" * 60)

                # Sử dụng select để chờ socket - hiệu quả hơn polling liên tục
                while True:
                    # nếu socket có activity trong 5s, trả về list non-empty
                    ready = select.select([self.connection], [], [], 5)
                    if ready[0]:
                        # có dữ liệu: poll và lấy notifications
                        self.connection.poll()
                        while self.connection.notifies:
                            notify = self.connection.notifies.pop(0)
                            message_count += 1
                            try:
                                self.handle_notification(notify, message_count)
                            except Exception:
                                logger.exception("Error processing notification")
                    else:
                        # timeout: không có notify; vòng lặp tiếp tục (đã có keepalive ở TCP level)
                        pass

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                # lỗi kết nối DB: cố reconnect
                logger.error(f"Postgres connection error: {e}")
                logger.info("Attempting to reconnect to PostgreSQL in %s seconds...", postgres_backoff)
                self._close_connection()
                time.sleep(postgres_backoff)
                postgres_backoff = min(postgres_backoff * 2, 30)
                continue
            except KeyboardInterrupt:
                logger.info("Interrupted by user")
                break
            except Exception as e:
                logger.exception("Unexpected error in listen loop: %s", e)
                # giữ kết nối / thử lại
                time.sleep(5)

    # ---------------------- Helpers ----------------------
    def _close_connection(self):
        try:
            if self.connection:
                try:
                    self.connection.close()
                except Exception:
                    pass
        finally:
            self.connection = None

    def shutdown(self):
        logger.info("Shutting down producer...")
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
                logger.info("Kafka producer closed")
        except Exception:
            logger.exception("Error closing Kafka producer")
        try:
            if self.connection:
                self.connection.close()
                logger.info("PostgreSQL connection closed")
        except Exception:
            logger.exception("Error closing DB connection")

    # ---------------------- Start toàn bộ ----------------------
    def start(self):
        try:
            logger.info("🚀 Starting PostgresKafkaProducer...")
            self.connect_kafka()
            self.connect_postgres()
            self.initial_load()
            self.listen_and_produce()
        except KeyboardInterrupt:
            logger.info("Shutting down (keyboard interrupt)")
        except Exception:
            logger.exception("Fatal error in producer")
            raise
        finally:
            self.shutdown()


if __name__ == "__main__":
    producer = PostgresKafkaProducer()
    producer.start()
