from datetime import datetime
from logging import Logger
from dds_loader.repository import DdsRepository
#from dds_loader.repository.category_class import Category
from dds_loader.repository.links_class import Links
from dds_loader.repository.satellites_class import Satellites
from dds_loader.repository.dds_repository import OrderDdsBuilder
from dds_loader.repository.kafka_connectors import KafkaConsumer, KafkaProducer
from app_config import AppConfig

class DdsMessageProcessor:
    def __init__(self,
                 logger: Logger,
                 dds_repository:DdsRepository,
                 consumer: KafkaConsumer,
                 producer: KafkaProducer,
                 batch_size: int) -> None:

        self._logger = logger
        self._dds_repository = dds_repository
        self._consumer = consumer
        self._producer = producer
        self._batch_size = batch_size
        
    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START. batch_size = 100, interval=5, max_instances=52 другим инетом")
        # вызываю данные из stg
        self._logger.info(f"{datetime.utcnow()}: getting data from stg")
        
        #self._logger.info(f'Что летит в builder. Мой словарь с данными {data_from_stg}')
        
        data_from_stg = self._dds_repository.get_data_from_stg()
 
        builder = OrderDdsBuilder(data_from_stg)
        
        self._logger.info('Началась вставка данных в хабы')
        for user, restaurant, product, order, category in zip(builder.h_user(), builder.h_restaurant(), 
                                                              builder.h_product(), builder.h_order(), 
                                                              builder.h_category()):
            self._dds_repository.insert_h_user(user)
            self._dds_repository.insert_h_restaurant(restaurant)
            self._dds_repository.insert_h_product(product)
            self._dds_repository.insert_h_order(order)
            self._dds_repository.insert_h_category(category)
        
        self._logger.info('Закончилась вставка данных в хабы')
        
        dds_message_list = self._dds_repository.dds_message(data_from_stg)
        for message in dds_message_list:
            self._logger.info(f'Сообщение летит {type(message), message}')
            self._producer.produce(message)
            self._logger.info(f'Сообщение улетело')

        self._logger.info('Началась вставка данных в таблицы-линки')
        for lop, lou, lpc, lpr in zip(builder.l_order_product(), builder.l_order_user(),
                                      builder.l_product_category(),
                                      builder.l_product_restaurant()):
            self._dds_repository.insert_l_order_product(lop)
            self._dds_repository.insert_l_order_user(lou)
            self._dds_repository.insert_l_product_category(lpc)
            self._dds_repository.insert_l_product_restaurant(lpr) 
        self._logger.info('Закочилась вставка данных в таблицы-линки')
        

        self._logger.info('Началась вставка данных в таблицы-сателлиты')
        
        for sun, srn, spn, sos, soc in zip(builder.s_user_names(), builder.s_restaurant_names(),
                                      builder.s_product_names(), builder.s_order_status(),
                                      builder.s_order_cost()):
            self._dds_repository.insert_s_user_names(sun)
            self._dds_repository.insert_s_restaurant_names(srn)
            self._dds_repository.insert_s_product_names(spn)
            self._dds_repository.insert_s_order_status(sos)
            self._dds_repository.insert_s_order_cost(soc)
        
        self._logger.info('Закончилась вставка данных в таблицы-сателлиты')
        

        
        