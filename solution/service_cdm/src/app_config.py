import os

from lib.kafka_connect import KafkaConsumer, KafkaProducer
from lib.pg import PgConnect



class AppConfig:
    CERTIFICATE_PATH = '/crt/YandexInternalRootCA.crt'
    DEFAULT_JOB_INTERVAL = 25

    def __init__(self) -> None:

        self.kafka_host = str(os.getenv('KAFKA_HOST') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_port = int(os.getenv('KAFKA_PORT', 'Скрыл по просьбе Яндекс Практикума')) if os.getenv('KAFKA_PORT') is None else int(os.getenv('KAFKA_PORT'))
        self.kafka_consumer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_consumer_topic = str(os.getenv('KAFKA_SOURCE_TOPIC') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_producer_username = str(os.getenv('KAFKA_CONSUMER_USERNAME') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_producer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD') or "Скрыл по просьбе Яндекс Практикума")
        self.kafka_producer_topic = str(os.getenv('KAFKA_DESTINATION_TOPIC') or "Скрыл по просьбе Яндекс Практикума")

        
        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST') or "Скрыл по просьбе Яндекс Практикума")
        self.pg_warehouse_port = int(os.getenv('PG_WAREHOUSE_PORT', '6432')) if os.getenv('PG_WAREHOUSE_PORT') is None else int(os.getenv('PG_WAREHOUSE_PORT'))
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME') or "Скрыл по просьбе Яндекс Практикума")
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER') or "Скрыл по просьбе Яндекс Практикума")
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD') or "Скрыл по просьбе Яндекс Практикума")

    def kafka_producer(self) -> KafkaProducer:
        return KafkaProducer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_producer_username,
            self.kafka_producer_password,
            self.kafka_producer_topic,
            self.CERTIFICATE_PATH
        )

    def kafka_consumer(self) -> KafkaConsumer:
        return KafkaConsumer(
            self.kafka_host,
            self.kafka_port,
            self.kafka_consumer_username,
            self.kafka_consumer_password,
            self.kafka_consumer_topic,
            self.kafka_consumer_group,
            self.CERTIFICATE_PATH
        )

    
    def pg_warehouse_db(self) -> PgConnect:
        return PgConnect(
            self.pg_warehouse_host,
            self.pg_warehouse_port,
            self.pg_warehouse_dbname,
            self.pg_warehouse_user,
            self.pg_warehouse_password
        )
