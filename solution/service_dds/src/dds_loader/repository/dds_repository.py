import uuid
from datetime import datetime
from typing import Any, Dict, List
import logging
from lib.pg import PgConnect
from pydantic import BaseModel
import json

class H_User(BaseModel):
    h_user_pk: uuid.UUID
    user_id: str
    load_dt: datetime
    load_src: str

class H_Restaurant(BaseModel):
    h_restaurant_pk: uuid.UUID
    restaurant_id: str
    load_dt: datetime
    load_src: str

class H_Product(BaseModel): 		
    h_product_pk: uuid.UUID
    product_id: str
    load_dt: datetime
    load_src: str

class H_Order(BaseModel):
    h_order_pk: uuid.UUID
    order_id: str
    order_dt: datetime
    load_dt: datetime
    load_src: str

class H_Category(BaseModel): 		
    h_category_pk: uuid.UUID
    category_name: str
    load_dt: datetime
    load_src: str


class L_OrderUser(BaseModel):
    hk_order_user_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_user_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_OrderProduct(BaseModel):
    hk_order_product_pk: uuid.UUID
    h_order_pk: uuid.UUID
    h_product_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class L_ProductCategory(BaseModel):
    hk_product_category_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_category_pk: uuid.UUID
    load_dt: datetime
    load_src: str

class L_ProductRestaurant(BaseModel):
    hk_product_restaurant_pk: uuid.UUID
    h_product_pk: uuid.UUID
    h_restaurant_pk: uuid.UUID
    load_dt: datetime
    load_src: str


class S_UserNames(BaseModel):
    h_user_pk: uuid.UUID
    username: str
    userlogin: str
    load_dt: datetime
    load_src: str
    hk_user_names_hashdiff: uuid.UUID


class S_RestaurantNames(BaseModel): 
    h_restaurant_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_restaurant_names_hashdiff: uuid.UUID


class S_ProductNames(BaseModel):
    h_product_pk: uuid.UUID
    name: str
    load_dt: datetime
    load_src: str
    hk_product_names_hashdiff: uuid.UUID

class S_OrderStatus(BaseModel):
    h_order_pk: uuid.UUID
    status: str
    load_dt: datetime
    load_src: str
    hk_order_status_hashdiff: uuid.UUID

class S_OrderCost(BaseModel):
    h_order_pk: uuid.UUID
    cost: float
    payment: float
    load_dt: datetime
    load_src: str
    hk_order_cost_hashdiff: uuid.UUID


class OrderDdsBuilder:
    def __init__(self, list: List) -> None:
        self._list = list
        self.source_system = "orders-system-kafka"
        self.order_ns_uuid = uuid.UUID('7f288a2e-0ad0-4039-8e59-6c9838d87307')
    
    def _uuid(self, obj: Any) -> uuid.UUID:
        return uuid.uuid5(namespace=self.order_ns_uuid, name=str(obj))

    def h_user(self) -> List[H_User]:
        user_info_list = []
        for user in self._list:
            user_id = user['payload']['user']['id']
            user_info_list.append(H_User(
                                        h_user_pk=self._uuid(user_id),
                                        user_id=user_id,
                                        load_dt = datetime.utcnow(),
                                        load_src = self.source_system
            ))
            
        return user_info_list
    

    def h_restaurant(self) -> List[H_Restaurant]:
        restaurant_info_list = []
        for restaurant in self._list:
            restaurant_id = restaurant['payload']['restaurant']['id']
            restaurant_info_list.append(H_Restaurant(
                h_restaurant_pk = self._uuid(restaurant_id),
                restaurant_id=restaurant_id,
                load_dt = datetime.utcnow(),
                load_src = self.source_system
            ))

        return restaurant_info_list
    
    def h_product(self) -> List[H_Product]:
        product_info_list = []
        products = []
        product_id = None
        for data in self._list:
            products= data['payload']['products']
            for product in products:
                product_id = product['id'] 	
                product_info_list.append(H_Product(h_product_pk=self._uuid(product_id),
                                                   product_id=product_id,
                                                   load_dt=datetime.utcnow(),
                                                   load_src=self.source_system))
        
        return product_info_list
            
    def h_order(self) -> List[H_Order]:
        order_info_list = []
        order_id = None
        for order in self._list:
            order_id = str(order['order_id'])
            order_info_list.append(H_Order(h_order_pk=self._uuid(order_id),
                                           order_id=order_id,
                                           order_dt=order['payload']['date'],
                                           load_dt=datetime.utcnow(),
                                           load_src=self.source_system))
        return order_info_list
            
        
    def h_category(self) -> List[H_Category]:
        category_info_list = [] 	
        for data in self._list:
            products= data['payload']['products']
            for product in products:
                category_name = product['category'] 	
                category_info_list.append(H_Category(h_category_pk=self._uuid(category_name),
                                                 category_name=category_name,	
                                                 load_dt=datetime.utcnow(),	
                                                 load_src=self.source_system))
        return category_info_list

    def l_order_product(self) -> List[L_OrderProduct]: 			
        l_order_product_list = []
        order_id = None
        product_id = None
        for order in self._list:
            order_id = order['order_id']
            products=order['payload']['products']
            for product in products:
                product_id=product['id']
                l_order_product_list.append(L_OrderProduct(hk_order_product_pk=self._uuid(f"{order_id}#$#{product_id}"),	
                                                           h_order_pk=self._uuid(order_id),
                                                           h_product_pk=self._uuid(product_id),
                                                           load_dt=datetime.utcnow(),	
                                                           load_src=self.source_system))



        return l_order_product_list


    def l_order_user(self) -> List[L_OrderUser]:
        l_order_user_list = []
        order_id = None
        user_id = None
        for order in self._list:
            order_id = order['order_id']
            user_id = order['payload']['user']['id']
            l_order_user_list.append(L_OrderUser(
                hk_order_user_pk=self._uuid(f"{order_id}$#${user_id}"),
                h_order_pk=self._uuid(order_id),
                h_user_pk=self._uuid(user_id),
                load_dt=datetime.utcnow(),	
                load_src=self.source_system
            ))
            
        return l_order_user_list
        

    def l_product_category(self) -> List[L_ProductCategory]: 			
        l_product_category_list = []
        product_id = None
        category_name = None
        for data in self._list:
            products=data['payload']['products']
            for product in products:
                product_id = product['id']
                category_name = product['category']
                l_product_category_list.append(L_ProductCategory(
                    hk_product_category_pk=self._uuid(f"{product_id}$#${category_name}"),
                    h_product_pk=self._uuid(product_id),
                    h_category_pk=self._uuid(category_name),
                    load_dt=datetime.utcnow(),	
                    load_src=self.source_system
                    ))
        return l_product_category_list
        
    def l_product_restaurant(self) -> List[L_ProductRestaurant]:		
        l_product_restaurant_list = []
        product_id = None
        restaurant_id = None
        for data in self._list:
            restaurant_id=data['payload']['restaurant']['id']
            products = data['payload']['products']
            for product in products:
                product_id = product['id']
                l_product_restaurant_list.append(L_ProductRestaurant(
                    hk_product_restaurant_pk=self._uuid(f"{product_id}$#${restaurant_id}"),
                    h_product_pk=self._uuid(product_id),
                    h_restaurant_pk=self._uuid(restaurant_id),
                    load_dt=datetime.utcnow(),	
                    load_src=self.source_system
                ))

        return l_product_restaurant_list
        
    def s_user_names(self) -> List[S_UserNames]:
        s_user_names_list = []
        user_id = None
        username = None
        userlogin = None

        for user in self._list:
            user_id = user['payload']['user']['id']
            username=user['payload']['user']['name']
            userlogin=user['payload']['user']['login']
            s_user_names_list.append(S_UserNames(
                h_user_pk=self._uuid(user_id),
                username=username,	
                userlogin=userlogin,
                load_dt=datetime.utcnow(),	
                load_src=self.source_system,
                hk_user_names_hashdiff=self._uuid(f"{user_id}$#${username}$#${userlogin}")
            ))


        return s_user_names_list
        
    def s_restaurant_names(self) -> List[S_RestaurantNames]:
        s_restaurant_names_list = []
        restaurant__id = None
        restaurant_name= None
        for restaurant in self._list:
            restaurant__id=restaurant['payload']['restaurant']['id']
            restaurant_name=restaurant['payload']['restaurant']['name']
            s_restaurant_names_list.append(S_RestaurantNames(
                h_restaurant_pk=self._uuid(restaurant__id),
                name=restaurant_name,
                load_dt=datetime.utcnow(),	
                load_src=self.source_system,
                hk_restaurant_names_hashdiff=self._uuid(f"{restaurant__id}$#${restaurant_name}")
            ))
            
        return s_restaurant_names_list
    

    def s_product_names(self) -> List[S_ProductNames]:
        s_product_names_list = []
        product_id = None
        product_name = None
        for data in self._list:
            products = data['payload']['products']
            for product in products:
                product_id = product['id']
                product_name=product['name']
                s_product_names_list.append(S_ProductNames(
                    h_product_pk=self._uuid(product_id),
                    name=product_name,
                    load_dt=datetime.utcnow(),	
                    load_src=self.source_system,
                    hk_product_names_hashdiff=self._uuid(f"{product_id}$#${product_name}")
                ))
                
        return s_product_names_list
    
    def s_order_status(self) -> List[S_OrderStatus]:
        s_order_status_list = []
        order_id = None
        order_status = None
        for order in self._list:
            order_id = str(order['order_id'])
            order_status = order['payload']['status']
            s_order_status_list.append(S_OrderStatus(
                h_order_pk=self._uuid(order_id),
                status=order_status,
                load_dt=datetime.utcnow(),	
                load_src=self.source_system,	
                hk_order_status_hashdiff=self._uuid(f"{order_id}$#${order_status}")
            ))

        return s_order_status_list
    
    def s_order_cost(self) -> List[S_OrderCost]:
        s_order_cost_list = []
        order_id = None
        order_cost = None
        order_payment = None
        for order in self._list:
            order_id = order['order_id']
            order_cost=order['payload']['cost']
            order_payment = order['payload']['payment']
            s_order_cost_list.append(S_OrderCost(
                h_order_pk=self._uuid(order_id),
                cost=order_cost,
                payment=order_payment,
                load_dt=datetime.utcnow(),	
                load_src=self.source_system,
                hk_order_cost_hashdiff=self._uuid(f"{order_id}$#${order_cost}$#${order_payment}")
            ))

        return s_order_cost_list



class DdsRepository:
    def __init__(self, db: PgConnect,  logger: logging.Logger) -> None:
        self._db = db
        self._logger = logger
        

    def get_data_from_stg(self):
         with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute('''
                                SELECT object_id,
                                        payload
                                FROM stg.order_events;
                            ''')
                data_from_stg = cur.fetchall()
            
            data_from_stg_list = []
            for data in data_from_stg:
                data_dict = {
                    "order_id":data[0],
                    "payload":data[1]
                }
                data_from_stg_list.append(data_dict)
            
            
            return data_from_stg_list
    
    def dds_message(self, data_from_stg):
        dds_message_list = []
        product_info = {}
        dds_message = {}
        product_list = []
        for data in data_from_stg:
            order_id = data['order_id']
            user_info = data['payload']['user']
            product_data_from_stg = data['payload']['products']
            for product in product_data_from_stg:
                product_info = {
                        "user": user_info,
                        "products":product
                }
                dds_message_list.append(product_info)
        
        return dds_message_list
            
    
    def insert_h_user(self, user: H_User) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_user(
                            h_user_pk,
                            user_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(user_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_user_pk) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user.h_user_pk,
                        'user_id': user.user_id,
                        'load_dt': user.load_dt,
                        'load_src': user.load_src
                    }
                )
        

    def insert_h_restaurant(self, restaurant: H_Restaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_restaurant(
                            h_restaurant_pk,
                            restaurant_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(restaurant_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant.h_restaurant_pk,
                        'restaurant_id': restaurant.restaurant_id,
                        'load_dt': restaurant.load_dt,
                        'load_src': restaurant.load_src
                    }
                )
    
    def insert_h_order(self, order: H_Order) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_order(
                            h_order_pk,
                            order_id,
                            order_dt,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(order_id)s,
                            %(order_dt)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_order_pk) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order.h_order_pk,
                        'order_id': order.order_id,
                        'order_dt': order.order_dt,
                        'load_dt': order.load_dt,
                        'load_src': order.load_src
                    }
                )

    def insert_h_product(self, product: H_Product) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_product(
                            h_product_pk,
                            product_id,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(product_id)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_product_pk) DO NOTHING;
                    """,
                    {
                        'h_product_pk': product.h_product_pk,
                        'product_id': product.product_id,
                        'load_dt': product.load_dt,
                        'load_src': product.load_src
                    }
                )     


    def insert_h_category(self, category: H_Category) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.h_category(
                            h_category_pk,
                            category_name,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(h_category_pk)s,
                            %(category_name)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (h_category_pk) DO NOTHING;
                    """,
                    {
                        'h_category_pk': category.h_category_pk,
                        'category_name': category.category_name,
                        'load_dt': category.load_dt,
                        'load_src': category.load_src
                    }
                )     
    

    def insert_l_order_user(self, order_user: L_OrderUser) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_user( 			
                            hk_order_user_pk, 
                            h_order_pk,
                            h_user_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_user_pk)s,
                            %(h_order_pk)s,
                            %(h_user_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_user_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_user_pk': order_user.hk_order_user_pk,
                        'h_order_pk': order_user.h_order_pk,
                        'h_user_pk': order_user.h_user_pk,
                        'load_dt': order_user.load_dt,
                        'load_src': order_user.load_src
                    }
                )     
    
    def insert_l_order_product(self, order_product: L_OrderProduct) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_order_product(
                            hk_order_product_pk, 
                            h_order_pk,
                            h_product_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_order_product_pk)s,
                            %(h_order_pk)s,
                            %(h_product_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_order_product_pk) DO NOTHING;
                    """,
                    {
                        'hk_order_product_pk': order_product.hk_order_product_pk,
                        'h_order_pk': order_product.h_order_pk,
                        'h_product_pk': order_product.h_product_pk,
                        'load_dt': order_product.load_dt,
                        'load_src': order_product.load_src
                    }
                )     


    def insert_l_product_category(self, product_category: L_ProductCategory) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_category(
                            hk_product_category_pk, 
                            h_product_pk,
                            h_category_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_category_pk)s,
                            %(h_product_pk)s,
                            %(h_category_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_category_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_category_pk': product_category.hk_product_category_pk,
                        'h_product_pk': product_category.h_product_pk,
                        'h_category_pk': product_category.h_category_pk,
                        'load_dt': product_category.load_dt,
                        'load_src': product_category.load_src
                    }
                )


    def insert_l_product_restaurant(self, product_restaurant: L_ProductRestaurant) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.l_product_restaurant(
                            hk_product_restaurant_pk, 
                            h_product_pk,
                            h_restaurant_pk,
                            load_dt,
                            load_src
                        )
                        VALUES(
                            %(hk_product_restaurant_pk)s,
                            %(h_product_pk)s,
                            %(h_restaurant_pk)s,
                            %(load_dt)s,
                            %(load_src)s
                        )
                        ON CONFLICT (hk_product_restaurant_pk) DO NOTHING;
                    """,
                    {
                        'hk_product_restaurant_pk': product_restaurant.hk_product_restaurant_pk,
                        'h_product_pk': product_restaurant.h_product_pk,
                        'h_restaurant_pk': product_restaurant.h_restaurant_pk,
                        'load_dt': product_restaurant.load_dt,
                        'load_src': product_restaurant.load_src
                    }
                )     

    def insert_s_user_names(self, user_names: S_UserNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_user_names(
                            h_user_pk, 
                            username,
                            userlogin,
                            load_dt,
                            load_src,
                            hk_user_names_hashdiff
                        )
                        VALUES(
                            %(h_user_pk)s,
                            %(username)s,
                            %(userlogin)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_user_names_hashdiff)s
                        )
                        ON CONFLICT (h_user_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_user_pk': user_names.h_user_pk,
                        'username': user_names.username,
                        'userlogin': user_names.userlogin,
                        'load_dt': user_names.load_dt,
                        'load_src': user_names.load_src,
                        'hk_user_names_hashdiff': user_names.hk_user_names_hashdiff
                    }
                )     
    
    def insert_s_restaurant_names(self, restaurant_names: S_RestaurantNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_restaurant_names(
                            h_restaurant_pk, 
                            name,
                            load_dt,
                            load_src,
                            hk_restaurant_names_hashdiff
                        )
                        VALUES(
                            %(h_restaurant_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_restaurant_names_hashdiff)s
                        )
                        ON CONFLICT (h_restaurant_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_restaurant_pk': restaurant_names.h_restaurant_pk,
                        'name': restaurant_names.name,
                        'load_dt': restaurant_names.load_dt,
                        'load_src': restaurant_names.load_src,
                        'hk_restaurant_names_hashdiff': restaurant_names.hk_restaurant_names_hashdiff
                    }
                )     
    
    def insert_s_product_names(self, product_names: S_ProductNames) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_product_names(
                            h_product_pk, 
                            name,
                            load_dt,
                            load_src,
                            hk_product_names_hashdiff
                        )
                        VALUES(
                            %(h_product_pk)s,
                            %(name)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_product_names_hashdiff)s
                        )
                        ON CONFLICT (h_product_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_product_pk': product_names.h_product_pk,
                        'name': product_names.name,
                        'load_dt': product_names.load_dt,
                        'load_src': product_names.load_src,
                        'hk_product_names_hashdiff': product_names.hk_product_names_hashdiff
                    }
                )

    def insert_s_order_status(self, order_status: S_OrderStatus) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_status(
                            h_order_pk, 
                            status,
                            load_dt,
                            load_src,
                            hk_order_status_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(status)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_status_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_status.h_order_pk,
                        'status': order_status.status,
                        'load_dt': order_status.load_dt,
                        'load_src': order_status.load_src,
                        'hk_order_status_hashdiff': order_status.hk_order_status_hashdiff
                    }
                )


    def insert_s_order_cost(self, order_cost: S_OrderCost) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO dds.s_order_cost(
                            h_order_pk, 
                            cost,
                            payment,
                            load_dt,
                            load_src,
                            hk_order_cost_hashdiff
                        )
                        VALUES(
                            %(h_order_pk)s,
                            %(cost)s,
                            %(payment)s,
                            %(load_dt)s,
                            %(load_src)s,
                            %(hk_order_cost_hashdiff)s
                        )
                        ON CONFLICT (h_order_pk, load_dt) DO NOTHING;
                    """,
                    {
                        'h_order_pk': order_cost.h_order_pk,
                        'cost': order_cost.cost,
                        'payment': order_cost.payment,
                        'load_dt': order_cost.load_dt,
                        'load_src': order_cost.load_src,
                        'hk_order_cost_hashdiff': order_cost.hk_order_cost_hashdiff
                    }
                )      
    

    