import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask

from app_config import AppConfig
from dds_loader.dds_message_processor_job import DdsMessageProcessor
from dds_loader.repository import DdsRepository
from dds_loader.repository.category_class import Category
from lib.pg.pg_connect import PgConnect
#from dds_loader.repository.links_class import Links
#from dds_loader.repository.satellites_class import Satellites
from dds_loader.repository.kafka_connectors import KafkaConsumer, KafkaProducer

app = Flask(__name__)

config = AppConfig()


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)
    
    
    

    proc = DdsMessageProcessor(logger=app.logger,
                               dds_repository=DdsRepository(db = config.pg_warehouse_db(), 
                                                            logger = app.logger
                                                            ),
                                consumer=config.kafka_consumer(),
                                producer=config.kafka_producer(),
                                batch_size=100
                                )
    


    scheduler = BackgroundScheduler({'apscheduler.job_defaults.max_instances': 52})
    scheduler.add_job(func=proc.run, trigger="interval", seconds=30)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
