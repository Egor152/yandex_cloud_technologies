import uuid
from datetime import datetime
from typing import Any, Dict, List
import logging
from lib.pg import PgConnect
from pydantic import BaseModel
from logging import Logger
import json

class CDM_UserProductCounters(BaseModel):
    user_id: uuid.UUID
    product_id: uuid.UUID
    product_name: str
    order_cnt: int

class CDM_UserCategoryCounters(BaseModel):
    user_id: uuid.UUID
    category_id: uuid.UUID
    category_name: str
    order_cnt: int


class CdmBuilder:
    def __init__(self, dict:Dict) -> None:
        self._dict = dict
        self.ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
    """Делаю UUID уже после получения данных, потому что TypeError: Object of type UUID is not JSON serializable
       Решил, что просто преобразую данные перед вставкой
    """
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.ns_uuid, name=str(obj))
                
    def user_product_counters(self) -> CDM_UserProductCounters:
        data_dict = self._dict
        user_id = data_dict['user']['id']
        product_id = data_dict['products']['id']
        product_name = data_dict['products']['name']
        return CDM_UserProductCounters(user_id=self._uuid(user_id),
                                           product_id=self._uuid(product_id),
                                           product_name=product_name,
                                           order_cnt=1)
    
    def user_category_counters(self)-> CDM_UserCategoryCounters:
        data_dict = self._dict
        user_id = data_dict['user']['id']
        category_name = data_dict['products']['category']
        return CDM_UserCategoryCounters(user_id=self._uuid(user_id),
                                        category_id=self._uuid(category_name),
                                        category_name=category_name,
                                        order_cnt=1)



class CdmRepository:
    def __init__(self, db: PgConnect,  logger: logging.Logger) -> None:
        self._db = db
        self._logger = logger

    def insert_user_product_counters(self, user_product_counters: CDM_UserProductCounters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_product_counters(
                            user_id,
                            product_id,	
                            product_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(product_id)s,
                            %(product_name)s,
                            %(order_cnt)s
                        )
                        ON CONFLICT (user_id, product_id) DO UPDATE SET
                        order_cnt = user_product_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': user_product_counters.user_id,
                        'product_id': user_product_counters.product_id,
                        'product_name': user_product_counters.product_name,
                        'order_cnt': user_product_counters.order_cnt
                    }
                )
    


    def insert_user_category_counters(self, user_category_counters: CDM_UserCategoryCounters) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO cdm.user_category_counters(
                            user_id,
                            category_id,	
                            category_name,
                            order_cnt
                        )
                        VALUES(
                            %(user_id)s,
                            %(category_id)s,
                            %(category_name)s,
                            %(order_cnt)s
                        )
                        ON CONFLICT (user_id, category_id) DO UPDATE SET
                        order_cnt = user_category_counters.order_cnt + 1;
                    """,
                    {
                        'user_id': user_category_counters.user_id,
                        'category_id': user_category_counters.category_id,
                        'category_name': user_category_counters.category_name,
                        'order_cnt': user_category_counters.order_cnt
                    }
                )
        
        