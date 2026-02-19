# async_db.py
# Асинхронная работа с базой данных. Использует глобальный пул соединений.

import asyncio
import json
import re
import os
from dotenv import load_dotenv
from datetime import datetime
import aiomysql
from aiomysql.cursors import DictCursor
import contextvars
import traceback

load_dotenv(dotenv_path=".env")


class Storage:
    _pool = None  # глобальный пул соединений

    @classmethod
    async def init_pool(cls, loop=None):
        """Инициализирует пул соединений (вызвать один раз при старте)"""
        if cls._pool is None:
            host = os.getenv('DB_HOST')
            user = os.getenv('DB_USER')
            password = os.getenv('DB_PASSWORD')
            db_name = os.getenv('DB_DATABASE')
            port = int(os.getenv('DB_PORT'))

            cls._pool = await aiomysql.create_pool(
                host=host,
                port=port,
                user=user,
                password=password,
                db=db_name,
                cursorclass=DictCursor,
                loop=loop or asyncio.get_event_loop(),
                minsize=1,
                maxsize=10  # можно настроить под нагрузку
            )
        return cls._pool

    @classmethod
    async def close_pool(cls):
        """Закрывает пул при завершении работы"""
        if cls._pool:
            cls._pool.close()
            await cls._pool.wait_closed()
            cls._pool = None

    def __init__(self, loop=None):
        self.loop = loop or asyncio.get_event_loop()
        # Ссылка на пул будет установлена в __aenter__
        self._pool = None

    async def __aenter__(self):
        # Убеждаемся, что пул существует (если не был инициализирован заранее)
        if Storage._pool is None:
            await self.init_pool(self.loop)
        self._pool = Storage._pool
        return self

    async def __aexit__(self, exc_type, exc, tb):
        # Ничего не закрываем – пул остаётся жить
        pass

    async def execute(self, query, args=None):
        """Выполняет запрос без возврата данных (INSERT, UPDATE, DELETE)"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, args or ())
                await conn.commit()

    async def executemany(self, query, args=None):
        """Выполняет множественный запрос"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany(query, args or [])
                await conn.commit()

    async def fetchall(self, query, *args):
        """Возвращает все строки результата"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, *args)
                return await cur.fetchall()

    async def fetchone(self, query, *args):
        """Возвращает одну строку результата"""
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(query, *args)
                return await cur.fetchone()

    # ---- Методы для работы с данными (остаются как есть, но используют новый пул) ----

    async def get_item_name_id(self, item: str):
        query = "SELECT item_name_id FROM item_name_ids WHERE item = %s"
        return await self.fetchone(query, (item,))

    async def add_item_name_id(self, item: str, item_name_id: int):
        query = "INSERT INTO item_name_ids (item, item_name_id) VALUES (%s, %s)"
        await self.execute(query, (item, item_name_id))

    async def set_item_info(self, market_name: str, buy_orders: list, sell_orders: list, history: list, appid: str = '440'):
        query = """INSERT IGNORE INTO prices (market_name, buy_orders, sell_orders, history, sales, appid) 
                   VALUES (%s, %s, %s, %s, %s, %s) 
                   ON DUPLICATE KEY UPDATE 
                       buy_orders = VALUES(buy_orders), 
                       sell_orders = VALUES(sell_orders), 
                       history = VALUES(history), 
                       sales = VALUES(sales), 
                       appid = VALUES(appid);"""
        sales = len(history) if history else 0
        appid = str(appid)
        await self.execute(query, (market_name, str(buy_orders), str(sell_orders), str(history), sales, appid))

    async def get_old_items(self, settings):
        hours = settings.hours
        appids = []
        if settings.CS:
            appids.append('730')
        if settings.TF2:
            appids.append('440')
        if settings.DOTA2:
            appids.append('570')

        appids_str = ', '.join(appids)

        query = f"SELECT market_name, appid FROM prices WHERE ts < NOW() - INTERVAL {hours} HOUR AND appid IN ({appids_str});"
        items = await self.fetchall(query)

        if len(items) < 100:
            query = f"SELECT market_name, appid FROM prices WHERE ts < NOW() - INTERVAL 1 HOUR AND appid IN ({appids_str}) LIMIT {100 - len(items)};"
            new_items = await self.fetchall(query)
            items.extend(new_items)

        return items

    async def get_item_price(self, market_name: str) -> dict:
        query = "SELECT * FROM prices WHERE market_name = %s"
        item = await self.fetchone(query, (market_name,))
        if item:
            history = eval(item['history'])
            buy_orders = eval(item['buy_orders'])
            sell_orders = eval(item['sell_orders'])
            sales = item['sales']
            return self.risky_prices(history, buy_orders, sell_orders, sales)
        else:
            return {}

    async def get_bought_price(self, market_name: str) -> dict:
        query = "SELECT * FROM market_history WHERE market_name = %s AND processed = 0 AND action = 'Purchased'"
        bought_price = await self.fetchone(query, (market_name,))
        return bought_price or {}

    @staticmethod
    def risky_prices(history: list[float], buy_orders: list[float], sell_orders: list[float], sales: int, lower_border_fl: float = 0.2) -> dict:
        history = sorted(history)
        lower_border = history[int(len(history) * lower_border_fl)]
        single_item_price = buy_orders[0]
        min_abs = abs(single_item_price - lower_border)

        buy_order_place = 0
        if lower_border_fl > 0:
            for order_place, buy_order in enumerate(buy_orders):
                temp_abs = abs(buy_order - lower_border)
                if temp_abs < min_abs:
                    min_abs = temp_abs
                    single_item_price = buy_order
                    buy_order_place = order_place
        else:
            buy_order_place = 100
            single_item_price = history[int(len(history) * 0.1)]

        sell_order_place = min([9, len(sell_orders) - 1, int(sales / 35)])
        try:
            price_to_sell = sell_orders[sell_order_place]
        except:
            price_to_sell = 0

        return {
            'buy_price': round(single_item_price + 0.03, 2),
            'sell_price': round(price_to_sell - 0.03, 2),
            'buy_order_place': buy_order_place,
            'sell_order_place': sell_order_place,
            'max_sell_order': max(sell_orders),
            'buy_orders': buy_orders,
            'sell_orders': sell_orders,
            'history': history,
            'sales': sales
        }

    async def get_all_items(self, settings):
        appids = []
        if settings.CS:
            appids.append('730')
        if settings.TF2:
            appids.append('440')
        if settings.DOTA2:
            appids.append('570')

        appids_str = ', '.join(appids)
        query = f"SELECT * FROM prices WHERE appid IN ({appids_str});"
        items = await self.fetchall(query)
        return items

    async def get_card_names(self, appid):
        query = "SELECT cards FROM game_cards WHERE appid = %s"
        result = await self.fetchone(query, (appid,))
        return result

    async def set_game_cards(self, appid: str, cards: list[str]):
        query = "INSERT INTO game_cards (appid, cards) VALUES (%s, %s)"
        await self.execute(query, (appid, str(cards)))

    async def dump_market_history(self, items: list[dict]):
        items_list = []
        for item in items:
            market_id = item['market_id']
            price = item['price']
            market_name = item['market_name']
            action, date_str = item['combined_date'].split(': ')
            current_year = datetime.now().year
            date_obj = datetime.strptime(f"{date_str} {current_year}", "%d %b %Y")
            items_list.append((market_name, market_id, price, action, date_obj))
        query = "INSERT IGNORE INTO market_history (market_name, market_id, price, action, ts) VALUES (%s, %s, %s, %s, %s)"
        await self.executemany(query, items_list)

    async def process_raw_data(self):
        query = "SELECT * FROM market_history WHERE processed = 0"
        items = await self.fetchall(query)

        bought_items = {}
        sold_items = {}
        while items:
            item = items.pop(0)
            action = item['action']
            market_name = item['market_name']
            if action == 'Purchased':
                bought_items.setdefault(market_name, []).append(item)
            elif action == 'Sold':
                sold_items.setdefault(market_name, []).append(item)

        transactions = []
        for market_name in sold_items:
            sold_temp = sold_items[market_name]
            if market_name in bought_items:
                bought_temp = bought_items[market_name]
                transactions.extend(zip(bought_temp, sold_temp))
        for item_pair in transactions:
            id1 = item_pair[0]['market_id']
            id2 = item_pair[1]['market_id']
            market_name = item_pair[0]['market_name']
            buy_price = item_pair[0]['price']
            buy_ts = item_pair[0]['ts']
            sell_price = item_pair[1]['price']
            sell_ts = item_pair[1]['ts']
            percent = round((sell_price / buy_price - 1) * 100, 2)

            query = "INSERT INTO transactions (market_name, buy_price, sell_price, buy_ts, sell_ts, percent) VALUES (%s, %s, %s, %s, %s, %s)"
            await self.execute(query, (market_name, buy_price, sell_price, buy_ts, sell_ts, percent))

            query = "UPDATE market_history SET processed = 1 WHERE market_id = %s"
            await self.execute(query, (id1,))
            await self.execute(query, (id2,))

    @classmethod
    async def log(cls, message: str, level: str = "INFO", module: str = "unknown"):
        """
        Асинхронная запись лога в БД. Может быть вызвана как метод класса.
        Использует глобальный пул соединений.
        """
        if cls._pool is None:
            # Если пул не инициализирован, создаём временный (для обратной совместимости)
            await cls.init_pool()
        async with cls._pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "INSERT INTO logs (message, level, module, ts) VALUES (%s, %s, %s, NOW())",
                    (message, level, module)
                )
                await conn.commit()

    async def get_all_items_from_market_history(self):
        query = "SELECT * FROM transactions WHERE id > 1000"
        items = await self.fetchall(query)
        return items

    async def get_settings(self, login):
        query = "SELECT * FROM settings WHERE login = %s"
        settings = await self.fetchone(query, (login,))
        return settings

    async def add_new_fetched(self, delay=12):
        query = "SELECT * FROM fetched_stat WHERE ts = %s"
        now = datetime.now()
        ts = now.strftime("%Y-%m-%d %H:%M") + ':00'
        fetched_info = await self.fetchone(query, (ts,))
        if fetched_info is None:
            query = "INSERT INTO fetched_stat (ts, fetched, delay) VALUES (%s, %s, %s)"
            await self.execute(query, (ts, 1, delay))
        else:
            fetched = fetched_info.get('fetched', 0) + 1
            query = "UPDATE fetched_stat SET fetched = %s, delay = %s WHERE ts = %s"
            await self.execute(query, (fetched, delay, ts))

    async def orders_update_needed(self, settings) -> bool:
        hours = settings.orders_update_time
        query = f"SELECT * FROM buy_orders WHERE ts < NOW() - INTERVAL {hours} HOUR"
        orders = await self.fetchall(query)
        return bool(orders)

    @staticmethod
    def get_appid(game_name):
        return {
            'Dota 2': 570,
            'Team Fortress 2': 440
        }.get(game_name)

    async def update_orders(self, orders):
        # Очищаем таблицы
        await self.execute("TRUNCATE buy_orders;")
        await self.execute("TRUNCATE sell_orders;")

        buy_orders_list = []
        for order in orders.get('buy_orders', {}).values():
            market_name = order.get('market_name')
            order_id = order.get('order_id')
            appid = self.get_appid(order.get('game_name'))
            quantity = order.get('quantity')
            price = float(order.get('price').replace(' руб.', '').replace(',', '.'))
            buy_orders_list.append((market_name, order_id, appid, quantity, price))

        if buy_orders_list:
            query = "INSERT IGNORE INTO buy_orders (market_name, order_id, appid, quantity, price) VALUES (%s, %s, %s, %s, %s)"
            await self.executemany(query, buy_orders_list)

        sell_orders_list = []
        for order in orders.get('sell_listings', {}).values():
            description = order.get('description')
            market_name = description.get('market_name')
            order_id = order.get('listing_id')
            appid = description.get('appid')
            raw_price = order.get('buyer_pay') or re.sub(r"\(.*\)", "", order.get('price')).strip()
            price = float(raw_price.replace(' руб.', '').replace(',', '.'))
            sell_orders_list.append((market_name, order_id, appid, 1, price))

        if sell_orders_list:
            query = "INSERT IGNORE INTO sell_orders (market_name, order_id, appid, quantity, price) VALUES (%s, %s, %s, %s, %s)"
            await self.executemany(query, sell_orders_list)

    async def get_orders(self):
        buy_orders = await self.fetchall("SELECT * FROM buy_orders")
        sell_listings = await self.fetchall("SELECT * FROM sell_orders")
        return {
            'buy_orders': buy_orders,
            'sell_listings': sell_listings
        }