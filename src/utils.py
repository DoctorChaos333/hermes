#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Вспомогательные функции для бота.
"""

import asyncio
import re
import traceback
from statistics import median_high
from datetime import datetime, timedelta

from steampy.models import GameOptions, Currency
import steampy.client
from bs4 import BeautifulSoup
import json
import time
from src.async_db import Storage


# Глобальная переменная для event loop (устанавливается из main.py)
_loop = None


def set_loop(loop):
    """Устанавливает event loop для синхронной обёртки log_sync."""
    global _loop
    _loop = loop


def log_sync(message: str, level: str = "INFO", module: str = "utils"):
    """
    Синхронная обёртка для записи лога в БД.
    Использует глобальный event loop, если он установлен, иначе создаёт временный.
    """
    try:
        if _loop is not None and not _loop.is_closed():
            _loop.run_until_complete(Storage.log(message, level, module))
        else:
            # Если цикл не задан или закрыт, запускаем асинхронно во временном цикле
            asyncio.run(Storage.log(message, level, module))
    except Exception as e:
        # В крайнем случае печатаем в консоль
        print(f"Не удалось записать лог: {e}")


# ---------- Остальные функции ----------

async def log_async(message: str, level: str = "INFO", module: str = "utils"):
    await Storage.log(message, level, module)

def history_link(item: str, appid: str = '730') -> str:
    """Формирует ссылку на историю цен предмета на Steam Market."""
    url = item.replace(' ', '%20').replace('#', '%23').replace(',', '%2C').replace('|', '%7C')
    return f"https://steamcommunity.com/market/listings/{appid}/{url}"


def median(data: list) -> float:
    """Безопасный расчет медианы с обработкой пустых данных."""
    if not data:
        raise ValueError("Cannot calculate median of empty list")
    return median_high(data)


class OrdersError(Exception):
    pass


async def get_old_items(settings) -> list[dict]:
    """Возвращает список предметов, которые давно не обновлялись."""
    async with Storage() as db:
        return await db.get_old_items(settings)


async def update_orders(orders):
    """Обновляет таблицы ордеров в БД."""
    async with Storage() as db:
        return await db.update_orders(orders)


async def orders_update_needed(settings):
    """Проверяет, нужно ли обновить ордера."""
    async with Storage() as db:
        return await db.orders_update_needed(settings)


async def get_orders():
    """Получает текущие ордера из БД."""
    async with Storage() as db:
        return await db.get_orders()


class Settings:
    def __init__(self, settings_info):
        self.__dict__.update(settings_info)


async def get_settings(login: str) -> Settings:
    """Получает настройки пользователя из БД."""
    async with Storage() as db:
        settings_info = await db.get_settings(login)
        return Settings(settings_info)


async def get_filtered_items(settings):
    """Возвращает отфильтрованный список предметов для выставления ордеров."""
    items = await get_all_items(settings)
    queue = items.copy()
    info = {
        'skins': 0,
        'not_fetched': 0,
        'not_profitable': 0,
        'all': len(items)
    }
    result = []
    while queue:
        item = queue.pop(0)
        if item.is_fetched(settings.hours):
            if item.is_profitable(settings.needed_percent):
                result.append(item)
                info['skins'] += 1
            else:
                if item.percent_below_market < 0.05:
                    info['not_profitable'] += 1
                    continue
                item.get_buy_price(item.percent_below_market - 0.01)
                queue.append(item)
        else:
            info['not_fetched'] += 1

    log_sync(f"Фильтрация предметов: {info}", "INFO", "utils")
    return result


async def get_all_items(settings):
    """Получает все предметы из БД и создаёт объекты Skin."""
    skins = []
    async with Storage() as db:
        all_items = await db.get_all_items(settings)
        for item in all_items:
            market_name = item['market_name']
            history = eval(item['history'])
            sales = item['sales']
            buy_orders = eval(item['buy_orders'])
            sell_orders = eval(item['sell_orders'])
            appid = str(item['appid'])
            ts = item['ts']
            skin = Skin(market_name, appid, history, buy_orders, sell_orders, sales, ts)
            skins.append(skin)
    return skins or []


async def get_item_price(market_name: str):
    async with Storage() as db:
        return await db.get_item_price(market_name)


async def get_risky_prices(history, buy_orders, sell_orders, sales):
    async with Storage() as db:
        return await db.risky_prices(history, buy_orders, sell_orders, sales)


async def get_bought_price(market_name: str):
    async with Storage() as db:
        return await db.get_bought_price(market_name)


async def dump_market_history(items: list[dict]) -> None:
    async with Storage() as db:
        await db.dump_market_history(items)


async def process_raw_data():
    async with Storage() as db:
        await db.process_raw_data()


def divide_list(lst: list, num):
    """Разделяет список на num примерно равных частей."""
    new_lst = [[] for _ in range(num)]
    i = 0
    while lst:
        element = lst.pop(0)
        new_lst[i].append(element)
        i = (i + 1) % num
    return new_lst


def fetch_inventory(inventory: dict):
    """Извлекает из инвентаря предметы, которые можно продать."""
    inventory_info = []
    for item_id, item_info in inventory.items():
        marketable = item_info['marketable']
        market_name = item_info['market_name']
        if marketable and market_name != "Mann Co. Supply Crate Key":
            inventory_info.append({
                'market_name': market_name,
                'item_id': item_id,
                'marketable': marketable
            })
    return inventory_info


def sell_items(steam_client: steampy.client.SteamClient):
    """Продаёт предметы из инвентаря."""
    games = [GameOptions.TF2, GameOptions.DOTA2]
    for game in games:
        for _ in range(2):
            try:
                inventory = fetch_inventory(steam_client.get_my_inventory(game, count=2000))
                time_left = 300
                for item in inventory:
                    assetid = item['item_id']
                    market_name = item['market_name']
                    item_info = asyncio.run(get_item_price(market_name))
                    price = item_info['sell_price']
                    sell_orders = item_info['sell_orders']
                    sell_order_place = item_info['sell_order_place']
                    bought_price = int((asyncio.run(get_bought_price(market_name))).get('price', 0) * 100)
                    money_to_receive = str(int(price * 87 - 3))

                    steam_client.market.create_sell_order(assetid, game, money_to_receive)
                    log_sync(f"Выставил {market_name} за {money_to_receive}. Ожидаемый минимум: {bought_price}",
                             "INFO", "utils")
                    time.sleep(4)
                    time_left -= 4

                for i in range(time_left, -1, -1):
                    # Здесь не логируем каждую секунду, чтобы не засорять БД
                    # Можно оставить print, если нужно
                    print(f'\rОсталось {i} секунд до нового запроса к инвентарю', end='', flush=True)
                    time.sleep(1)
                print()  # перевод строки после обратного отсчёта

            except Exception as e:
                log_sync(f"Не удалось получить инвентарь {game}: {traceback.format_exc()}", "ERROR", "utils")


def rub2float(price: str):
    """Преобразует строку с ценой в рубли в число с плавающей точкой."""
    return float(price.replace(',', '.').replace(' руб.', ''))


class Orders:
    """Класс для работы с ордерами (покупка/продажа)."""

    def __init__(self, orders_str, settings=None):
        self.buy_orders: list[Order] = []
        self.sell_listings: list[Order] = []
        self.settings = settings

        db_info_skins = Skins(asyncio.run(get_all_items(self.settings)))

        if not isinstance(orders_str, dict):
            orders = eval(orders_str)
        else:
            orders = orders_str

        # Обработка buy_orders
        if orders.get('buy_orders'):
            self.buy_orders = []
            for order in orders['buy_orders'].values():
                additional_info = db_info_skins.is_skin_there(order.get('market_name')) or {}
                order.update(additional_info.__dict__ if hasattr(additional_info, '__dict__') else {})
                self.buy_orders.append(Order(order))
        else:
            self.buy_orders = []

        # Обработка sell_listings
        if orders.get('sell_listings'):
            self.sell_listings = []
            for order in orders['sell_listings'].values():
                if 'listing_id' in order:
                    market_name = order.get('description').get('market_name')
                else:
                    market_name = order.get('market_name')
                additional_info = db_info_skins.is_skin_there(market_name) or {}
                if additional_info:
                    order.update(additional_info.__dict__ if hasattr(additional_info, '__dict__') else {})
                    self.sell_listings.append(Order(order))
                else:
                    log_sync(f"Нет дополнительной информации для {market_name}", "WARNING", "utils")
        else:
            self.sell_listings = []

    def update_settings(self, settings):
        if self.settings is None:
            self.settings = settings

    def cancel_buy_orders(self, settings, steam_client: steampy.client.SteamClient):
        """Отменяет ордера на покупку (заглушка)."""
        pass

    def set_buy_orders(self, settings, steam_client: steampy.client.SteamClient):
        """Выставляет ордера на покупку согласно настройкам."""
        log_sync("Начинаю простановку ордеров на покупку...", "INFO", "utils")
        filtered_items = Skins(asyncio.run(get_filtered_items(settings)))
        log_sync(f"Получено {len(filtered_items)} предметов для ордеров", "INFO", "utils")

        appids = []
        if settings.CS:
            appids.append('730')
        if settings.TF2:
            appids.append('440')
        if settings.DOTA2:
            appids.append('570')

        percent_below_info = {}
        counter = 0
        for skin in filtered_items:
            counter += 1
            market_name = skin.market_name
            order = self.is_order_there(market_name)
            percent_below = max(round(skin.percent_below_market, 2), 0.01)
            price_single_item = str(int(skin.buy_price * 100))
            quantity = max(0, int(9 * percent_below))
            if skin.buy_price > 100:
                quantity = min(quantity, 2)

            percent_below_info[percent_below] = percent_below_info.get(percent_below, 0) + 1

            # Если ордер уже стоит
            if order:
                if settings.low_percent * skin.buy_price <= order.price <= settings.high_percent * skin.buy_price:
                    log_sync(
                        f"Нормальный ордер | {market_name[:48]:48} | B: {skin.buy_price:6} | S: {skin.sell_price:6} | Q: {quantity:2} | %: {skin.Percent:2} | %B: {percent_below}",
                        "DEBUG", "utils")
                else:
                    log_sync(
                        f"Отмена ордера | {market_name[:48]:48} | B: {skin.buy_price:6} | S: {skin.sell_price:6} | Q: {quantity:2} | %: {skin.Percent:2} | %B: {percent_below}",
                        "INFO", "utils")
                    steam_client.market.cancel_buy_order(order.order_id)

                    if quantity:
                        time.sleep(5)
                        log_sync(
                            f"Создание ордера | {market_name[:48]:48} | B: {skin.buy_price:6} | S: {skin.sell_price:6} | Q: {quantity:2} | %: {skin.Percent:2} | %B: {percent_below}",
                            "INFO", "utils")
                        result = steam_client.market.create_buy_order(
                            market_name, price_single_item, quantity, skin.get_appid(), Currency.RUB
                        )
                        if result.get('success') != 1:
                            log_sync(f"Не удалось поставить ордер: {result}", "ERROR", "utils")
                            return
            else:
                if quantity:
                    time.sleep(5)
                    log_sync(
                        f"Создание ордера | {market_name[:48]:48} | B: {skin.buy_price:6} | S: {skin.sell_price:6} | Q: {quantity:2} | %: {skin.Percent:2} | %B: {percent_below}",
                        "INFO", "utils")
                    result = steam_client.market.create_buy_order(
                        market_name, price_single_item, quantity, skin.get_appid(), Currency.RUB
                    )
                    if result.get('success') != 1:
                        log_sync(f"Не удалось поставить ордер: {result}", "ERROR", "utils")
                        return

        # Удаление ордеров на предметы, которых нет в фильтрованном списке
        try:
            if filtered_items:
                for order in self.buy_orders:
                    market_name = order.market_name
                    skin = filtered_items.is_skin_there(market_name)
                    if not skin:
                        log_sync(f"Удаляю ордер на {market_name} (нет в фильтре)", "INFO", "utils")
                        steam_client.market.cancel_buy_order(order.order_id)
                        time.sleep(3)
            else:
                log_sync("База предметов старая, ордера не удаляю", "WARNING", "utils")
        except Exception as e:
            log_sync(f"Ошибка при удалении лишних ордеров: {traceback.format_exc()}", "ERROR", "utils")

    def cancel_sell_listings(self, steam_client: steampy.client.SteamClient):
        """Отменяет ордера на продажу, если цена слишком высока."""
        for item in self.sell_listings:
            market_name = item.market_name
            current_price = item.price
            needed_price = item.db_info.get('sell_price', 0)
            order_id = item.order_id

            if current_price > needed_price * self.settings.cancel_sell_listing_percent:
                log_sync(f"Убираю {market_name} | разница: {round(100 * current_price / needed_price - 100, 2)}%",
                         "INFO", "utils")
                try:
                    steam_client.market.cancel_sell_order(order_id)
                except Exception as e:
                    log_sync(f"Ошибка при отмене продажи {market_name}: {traceback.format_exc()}", "ERROR", "utils")
                time.sleep(10)

    def is_order_there(self, market_name: str):
        """Проверяет, есть ли уже ордер на покупку данного предмета."""
        for order in self.buy_orders:
            if order.market_name == market_name:
                return order
        return None


class Order:
    """Класс, представляющий один ордер (покупка или продажа)."""

    def __init__(self, data):
        self.market_name = None
        self.order_id = None
        self.quantity = None
        self.appid = None
        self.price = None
        self.history = None
        self.sell_orders = None
        self.buy_orders = None
        self.sales = None
        self.db_info = {}
        self.overpriced = False
        self.is_deep = False

        if not data:
            return

        # Для ордеров на покупку (из buy_orders)
        if 'listing_id' not in data:
            self.market_name = data.get('market_name')
            self.order_id = data.get('order_id')
            self.quantity = data.get('quantity')
            self.appid = self.get_appid(data.get('game_name'))
            self.price = self.get_price(data.get('price'))
            self.history = data.get('history')
            self.sell_orders = data.get('sell_orders')
            self.buy_orders = data.get('buy_orders')
            self.sales = data.get('sales')
        else:
            # Для ордеров на продажу (из sell_listings)
            self.description = data.get('description', {})
            self.market_name = self.description.get('market_name')
            self.order_id = data.get('listing_id')
            self.quantity = self.description.get('amount')
            self.appid = self.description.get('appid')
            price_str = data.get('buyer_pay') or re.sub(r"\(.*\)", "", data.get('price', '')).strip()
            self.price = self.get_price(price_str)
            self.history = data.get('history')
            self.sell_orders = data.get('sell_orders')
            self.buy_orders = data.get('buy_orders')
            self.sales = data.get('sales')

        # Рассчитываем рисковые цены
        if self.history and self.buy_orders and self.sell_orders and self.sales is not None:
            self.db_info = self.risky_prices(self.history, self.buy_orders, self.sell_orders, self.sales)
            self.is_deep = self._is_deep()

    def risky_prices(self, history: list[float], buy_orders: list[float], sell_orders: list[float], sales: int,
                     lower_border_fl: float = 0.2) -> dict:
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
            'sell_orders': sell_orders,
            'history': history
        }

    @staticmethod
    def get_appid(game_name):
        return {
            'Dota 2': 570,
            'Team Fortress 2': 440
        }.get(game_name)

    @staticmethod
    def get_price(price):
        if isinstance(price, (int, float)):
            return float(price)
        return float(price.replace(' руб.', '').replace(',', '.'))

    def _is_deep(self):
        buy_price = self.db_info.get('buy_price')
        if buy_price and self.price < buy_price and self.price > 0.8 * buy_price:
            return True
        return False

    def __repr__(self):
        return str(self.__dict__)


def update_my_market_history(steam_client: steampy.client.SteamClient):
    """Обновляет историю покупок/продаж аккаунта."""
    log_sync("Обновляю историю покупок и продаж...", "INFO", "utils")
    try:
        response = steam_client._session.get('https://steamcommunity.com/market/myhistory?count=500').text
        response_json = json.loads(response)
        html_content = response_json.get('results_html')
        items = parse_market_history(html_content)
        if items:
            asyncio.run(dump_market_history(items))
            log_sync(f"Добавлено {len(items)} записей в историю", "INFO", "utils")
    except Exception as e:
        log_sync(f"Ошибка при обновлении истории: {traceback.format_exc()}", "ERROR", "utils")


def parse_market_history(html_content):
    """Парсит HTML страницы истории маркета."""
    soup = BeautifulSoup(html_content, 'html.parser')
    items = []
    for row in soup.find_all('div', class_='market_listing_row'):
        item = {}
        market_id_element = row.attrs.get('id', None)
        if market_id_element:
            item['market_id'] = market_id_element.replace('history_row_', '')
        price_element = row.find('span', class_='market_listing_price')
        if price_element:
            price_text = price_element.get_text(strip=True)
            item['price'] = float(price_text.replace('руб.', '').replace(',', '.').strip())
        name_element = row.find('span', class_='market_listing_item_name')
        if name_element:
            item['market_name'] = name_element.get_text(strip=True)
        date_elements = row.find_all('div', class_='market_listing_listed_date')
        if len(date_elements) >= 2:
            item['listed_date'] = date_elements[0].get_text(strip=True)
            item['acted_date'] = date_elements[1].get_text(strip=True)
        combined_element = row.find('div', class_='market_listing_listed_date_combined')
        if combined_element:
            item['combined_date'] = combined_element.get_text(strip=True)
        items.append(item)
    return items


class Skin:
    """Класс, представляющий предмет (скин) с данными о ценах."""

    def __init__(self, market_name: str, appid: str, history: list[float], buy_orders: list[float],
                 sell_orders: list[float], sales: int, ts: datetime):
        self.market_name = market_name
        self.appid = appid
        self.history = history
        self.buy_orders = buy_orders
        self.sell_orders = sell_orders
        self.sales = sales
        self.buy_price = None
        self.sell_price = None
        self.percent_below_market = None
        self.Percent = None
        self.percent = None
        self.ts = ts

    def get_sell_price(self):
        if self.sell_price is None:
            sell_order_place = min([9, len(self.sell_orders) - 1, int(self.sales / 35)])
            if sell_order_place < 0:
                self.sell_price = 0
            else:
                try:
                    self.sell_price = self.sell_orders[sell_order_place]
                except:
                    self.sell_price = 0
        if self.Percent is None and self.buy_price:
            self.Percent = int(self.sell_price * 87 / self.buy_price - 100)
            self.percent = round(self.sell_price * 0.87 / self.buy_price)
        return self.sell_price

    def get_buy_price(self, percent_below_market: float = 0.5):
        history = sorted(self.history)
        lower_border = history[int(len(history) * percent_below_market)]
        single_item_price = self.buy_orders[0]
        min_abs = abs(single_item_price - lower_border)
        self.percent_below_market = round(percent_below_market, 2)

        if percent_below_market > 0:
            for order_place, buy_order in enumerate(self.buy_orders):
                temp_abs = abs(buy_order - lower_border)
                if temp_abs < min_abs:
                    min_abs = temp_abs
                    single_item_price = buy_order
        else:
            single_item_price = history[int(len(history) * 0.1)]

        self.buy_price = single_item_price

        if self.Percent is None and self.sell_price:
            self.Percent = int(self.sell_price * 87 / self.buy_price - 100)
            self.percent = round(self.sell_price * 0.87 / self.buy_price)

        return self.buy_price

    def x100price(self):
        return str(int(self.buy_price * 100 + 3))

    def is_profitable(self, k: float = 1.03):
        if not self.buy_price:
            self.get_buy_price()
        if not self.sell_price:
            self.get_sell_price()
        self.Percent = int(self.sell_price * 87 / self.buy_price - 100)
        self.percent = round(self.sell_price * 0.87 / self.buy_price)
        return self.sell_price * 0.87 / self.buy_price >= k

    def is_fetched(self, hours: int = 6) -> bool:
        return datetime.now() - self.ts < timedelta(hours=hours)

    def get_appid(self):
        return {
            '570': GameOptions.DOTA2,
            '440': GameOptions.TF2
        }.get(self.appid)

    def __repr__(self):
        return str(self.__dict__)


class Skins:
    """Коллекция объектов Skin с удобным доступом по имени."""

    def __init__(self, skins: list[Skin] = None):
        if skins is None:
            self.skins = []
        else:
            self.skins = skins
        self.skins_dict = {skin.market_name: skin for skin in skins}
        self._index = 0

    def is_skin_there(self, market_name):
        return self.skins_dict.get(market_name)

    def __len__(self):
        return len(self.skins_dict)

    def __repr__(self):
        return str(self.skins_dict)

    def __iter__(self):
        self._index = 0
        return self

    def __next__(self):
        if self._index < len(self.skins):
            result = self.skins[self._index]
            self._index += 1
            return result
        else:
            raise StopIteration