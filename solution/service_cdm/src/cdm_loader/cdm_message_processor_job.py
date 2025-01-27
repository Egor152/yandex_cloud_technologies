from datetime import datetime
from logging import Logger
from uuid import UUID

from cdm_loader.repository.kafka_connectors import KafkaConsumer, KafkaProducer
from cdm_loader.repository.cdm_repository import CdmBuilder, CdmRepository
from app_config import AppConfig

class CdmMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 batch_size: int,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 cdm_repository : CdmRepository
                 ) -> None:

        self._logger = logger
        self._batch_size = batch_size
        self._consumer = consumer
        self._producer = producer
        self._cdm_repository = cdm_repository
        

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")
        
        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            self._logger.info(f'Прилетело сообщение {msg}')
            if not msg:
                self._logger.info(f'СООБЩЕНИЕ ПУСТОЕ, ЕГО НЕТ')
                break
            
       
            cdm_builder = CdmBuilder(msg)
            

            self._cdm_repository.insert_user_product_counters(cdm_builder.user_product_counters())
            self._logger.info(f'Произошла вставка данных в cdm.user_product_counters')
            
            self._cdm_repository.insert_user_category_counters(cdm_builder.user_category_counters())
            self._logger.info(f'Произошла вставка данных в cdm.user_category_counters')
            
            
        self._logger.info(f"{datetime.utcnow()}: FINISH")
