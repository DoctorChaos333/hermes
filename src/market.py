#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для работы с торговой площадкой Steam.
Содержит классы Market (для получения данных) и MarketWorker (для многопоточного сканирования).
"""

import asyncio
import json
import re
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from typing import Optional, List

import aiohttp
import fake_useragent
from aiohttp import CookieJar, client_exceptions
from tqdm import tqdm

from src import async_db
from src.utils import history_link, OrdersError, log_async, get_settings

# Константы
DEFAULT_COUNTRY = "PL"
DEFAULT_CURRENCY = 5
DATE_FORMAT = "%b %d %Y %H: +0"
HISTORY_VAR_REGEX = r"var line1=(.+);"
MARKET_LOADORDERSPREAD_REGEX = r'\{ Market_LoadOrderSpread\( (\d*)'


class Market:
    """Класс для получения исторических данных и ордеров с торговой площадки Steam."""

    def __init__(self, session: Optional[aiohttp.ClientSession] = None):
        self.session = session or aiohttp.ClientSession()
        self.user_agent = fake_useragent.UserAgent()
        self.headers = {
            "User-Agent": self.user_agent.random,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

    async def fetch_history(
        self,
        item: str,
        appid: str = "730",
        days: int = 7,
        session: aiohttp.ClientSession = None
    ) -> List[float]:
        """Получает исторические данные цен для указанного предмета."""
        try:
            return await self._fetch_and_process_data(item, appid, days, session)
        except Exception as e:
            await log_async(f"Ошибка получения истории для {item}: {traceback.format_exc()}", "ERROR", "market")
            return []

    async def _fetch_and_process_data(
        self,
        item: str,
        appid: str,
        days: int,
        session: aiohttp.ClientSession
    ) -> List[float]:
        url = history_link(item, appid)
        params = self._build_history_params(item, appid)
        async with session.get(url, headers=self.headers, params=params) as response:
            response.raise_for_status()
            raw_data = await self._extract_historical_data(await response.text())
            return self._process_historical_data(raw_data, days)

    def _build_history_params(self, item: str, appid: str) -> dict:
        return {
            "country": DEFAULT_COUNTRY,
            "appid": appid,
            "market_hash_name": item,
            "currency": DEFAULT_CURRENCY,
        }

    def _build_orders_params(self, item: str, item_name_id: str) -> dict:
        return {
            "country": DEFAULT_COUNTRY,
            "language": "english",
            "item_nameid": item_name_id,
            "currency": DEFAULT_CURRENCY,
        }

    async def _extract_historical_data(self, response_text: str) -> List[tuple]:
        match = re.search(HISTORY_VAR_REGEX, response_text.strip())
        if not match:
            return []
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError:
            return []

    def _process_historical_data(self, raw_data: List[tuple], days: int) -> List[float]:
        if not raw_data:
            return []
        cutoff_date = datetime.now() - timedelta(days=days)
        prices = []
        for date_str, price, count in raw_data:
            try:
                entry_date = datetime.strptime(date_str, DATE_FORMAT)
                if entry_date >= cutoff_date:
                    prices.extend([float(price)] * int(count))
            except (ValueError, TypeError):
                continue
        return prices

    async def get_item_name_id(self, item: str, appid: str = "730") -> str:
        """Получает item_nameid для предмета, используя кеш в БД."""
        params = self._build_history_params(item, appid)
        url = history_link(item, appid)
        try:
            async with async_db.Storage() as db:
                is_presence = await db.get_item_name_id(item)
                if is_presence:
                    return is_presence["item_name_id"]
                await log_async(f"Предмета {item} нет в БД, запрашиваю...", "DEBUG", "market")
            async with self.session.get(url, headers=self.headers, params=params) as response:
                response.raise_for_status()
                response_text = await response.text()
                match_id = re.search(MARKET_LOADORDERSPREAD_REGEX, response_text)
                item_name_id = match_id.group(1)
                async with async_db.Storage() as db:
                    await db.add_item_name_id(item, item_name_id)
                return item_name_id
        except Exception as e:
            await log_async(f"Не удалось получить item_name_id для {item}: {traceback.format_exc()}", "ERROR", "market")
            raise

    async def fetch_history1(self, item: str, appid: int, session: aiohttp.ClientSession):
        """Альтернативный метод получения истории через pricehistory/ (не используется)."""
        params = {"country": "PL", "appid": appid, "market_hash_name": item}
        url = "https://steamcommunity.com/market/pricehistory/"
        async with session.get(url, headers=self.headers, params=params) as response:
            response_json = json.loads(await response.text())
            raw_data = response_json["prices"]
            return self._process_historical_data(raw_data, 7)

    async def get_orders(self, item: str, item_name_id: str):
        """Получает ордера на покупку и продажу для предмета."""
        params = self._build_orders_params(item, item_name_id)
        url = "https://steamcommunity.com/market/itemordershistogram"
        try:
            async with self.session.get(url, headers=self.headers, params=params) as response:
                response.raise_for_status()
                response_json = json.loads(await response.text())
                if not response_json.get("success"):
                    raise OrdersError("Не удалось получить данные по скину")
                buy_orders = []
                for price, count in response_json.get("buy_order_graph", []):
                    buy_orders.extend([price] * count)
                sell_orders = []
                for price, count in response_json.get("sell_order_graph", []):
                    sell_orders.extend([price] * count)
                return {
                    "buy_orders": buy_orders[:10],
                    "sell_orders": sell_orders[:10],
                }
        except client_exceptions.ClientResponseError as e:
            await log_async(f"Ошибка HTTP при получении ордеров для {item}: {e}", "ERROR", "market")
            await asyncio.sleep(10)
            return None
        except Exception as e:
            await log_async(f"Ошибка при получении ордеров для {item}: {traceback.format_exc()}", "ERROR", "market")
            return None

    @staticmethod
    async def set_item_info(market_name: str, buy_orders: list, sell_orders: list, history: list, appid: str):
        """Сохраняет информацию о предмете в БД."""
        try:
            async with async_db.Storage() as db:
                await db.set_item_info(market_name, buy_orders, sell_orders, history, appid)
        except Exception as e:
            await log_async(f"Ошибка сохранения {market_name}: {traceback.format_exc()}", "ERROR", "market")

    @staticmethod
    async def get_card_names(appid: str):
        """Получает список карточек игры из БД."""
        try:
            async with async_db.Storage() as db:
                return await db.get_card_names(appid)
        except Exception as e:
            await log_async(f"Ошибка получения card_names для {appid}: {traceback.format_exc()}", "ERROR", "market")
            return None

    async def get_market_listings(self):
        """Заглушка, не используется."""
        pass

    @staticmethod
    async def set_game_cards(appid: str, cards: list):
        """Сохраняет список карточек игры в БД."""
        try:
            async with async_db.Storage() as db:
                await db.set_game_cards(appid, cards)
        except Exception as e:
            await log_async(f"Ошибка сохранения card_names для {appid}: {traceback.format_exc()}", "ERROR", "market")

    @staticmethod
    async def analyze_item(item: str | dict, cookies, proxy: str = None, cookies_dict=None):
        """
        Анализирует один предмет: получает item_nameid, историю и ордера, сохраняет в БД.
        Возвращает словарь с результатом.
        """
        async with aiohttp.ClientSession(proxy=proxy) as session:
            session.cookie_jar.update_cookies(cookies)
            market = Market(session)
            if isinstance(item, dict):
                market_name = item["market_name"]
                appid = item["appid"]
            else:
                market_name = item
                appid = "730"
            try:
                item_name_id = await market.get_item_name_id(market_name, appid)
                history = await market.fetch_history(market_name, appid, session=session)
                if history:
                    orders = await market.get_orders(market_name, item_name_id)
                    if orders:
                        await market.set_item_info(
                            market_name,
                            orders["buy_orders"],
                            orders["sell_orders"],
                            history,
                            appid,
                        )
                        return {"success": 200}
            except Exception as e:
                await log_async(f"Ошибка анализа {market_name}: {traceback.format_exc()}", "ERROR", "market")
        return {"success": 429, "message": f"Прокси {proxy} нужно отдохнуть..."}

    @staticmethod
    async def get_cards_prices(link: str, cookies=None, proxy: str = None):
        """Получает названия карточек игры по ссылке на маркет (не используется активно)."""
        appid = re.search(r"(?:tag_app_|%5B%5D=tag_app_)(\d+)", link).group(1)
        market = Market()
        card_names = await market.get_card_names(appid)
        if card_names:
            await log_async(f"Карточки для {appid} найдены в БД", "DEBUG", "market")
        else:
            async with aiohttp.ClientSession(proxy=proxy) as session:
                session.cookie_jar.update_cookies(cookies)
                async with session.get(link) as response:
                    response.raise_for_status()
                    response_text = await response.text()
                    pattern = r'class="market_listing_item_name" style="color: #;">(.*?)</span>'
                    card_names = re.findall(pattern, response_text)
                    await market.set_game_cards(appid, card_names)
        return card_names


class MarketWorker:
    """
    Воркер, который в отдельной асинхронной задаче обрабатывает список предметов,
    используя один прокси и куки. Отображает прогресс через tqdm.
    """

    workers_info = OrderedDict()
    workers = []
    progress_bars = {}
    is_redistributing = False

    def __init__(self, proxy=None, cookies=None, cookies_dict=None, delay: int = 12, login=None):
        self.proxy = proxy or None
        self.cookies = cookies
        self.cookies_dict = cookies_dict
        self.tasks = []
        self.delay = delay
        self.login = login
        self.is_active = True
        self._create_progress_bar()

    def _create_progress_bar(self):
        """Создаёт прогресс-бар для воркера, если его ещё нет."""
        if self.proxy not in self.__class__.progress_bars:
            self.__class__.progress_bars[self.proxy] = tqdm(
                total=100,
                desc=f"{self.proxy}",
                bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                position=len(self.__class__.progress_bars),
                leave=True,
            )
            self.__class__.progress_bars[self.proxy].set_description_str(
                f"{self.proxy[:15]:<15} [Отдыхает]"
            )

    @classmethod
    def redistribute_items(cls):
        """Перераспределяет оставшиеся задачи между активными воркерами."""
        if any(worker.is_active for worker in cls.workers) and not cls.is_redistributing:
            cls.is_redistributing = True
            items = []
            for worker in cls.workers:
                while worker.tasks:
                    items.append(worker.tasks.pop(0))
            while items:
                for worker in cls.workers:
                    if items:
                        worker.tasks.append(items.pop(0))
                    else:
                        break
            cls.is_redistributing = False

    async def start_working(self):
        """Запускает обработку задач воркером."""
        exits = 0
        while self.tasks:
            settings = await get_settings(self.login)
            self.delay = settings.delay
            item = self.tasks.pop(0)

            self._change_status(
                status=f"Проверяет {item['market_name']} | Вылетов: {exits}",
                progress=0,
            )
            result = await Market.analyze_item(item, self.cookies, self.proxy, self.cookies_dict)

            if result["success"] != 200:
                self.is_active = False
                exits += 1
                rest = 25 * self.delay
                await log_async(
                    f"Прокси {self.proxy} получил ошибку, отдых {rest}с, вылетов: {exits}",
                    "WARNING",
                    "market",
                )
                for i in range(rest):
                    self._change_status(
                        status=f"X | Отдых {rest - i} с. Осталось: {len(self.tasks)}".ljust(50),
                        progress=int(i / rest * 100),
                    )
                    await asyncio.sleep(1)
                self.is_active = True
            else:
                async with async_db.Storage() as db:
                    await db.add_new_fetched(delay=self.delay)
                rest = self.delay
                await log_async(
                    f"Прокси {self.proxy} успешно обработал {item['market_name']}",
                    "DEBUG",
                    "market",
                )
                for i in range(rest):
                    self._change_status(
                        status=f"V | Отдых {rest - i} с. Осталось: {len(self.tasks)}".ljust(50),
                        progress=int(i / rest * 100),
                    )
                    await asyncio.sleep(1)

                if len(self.tasks) == 0:
                    self.redistribute_items()

        self._change_status("Отдых", 100)

    def add_tasks(self, tasks: list):
        """Добавляет задачи в очередь воркера."""
        self.tasks.extend(tasks)
        self._change_status(f"Получил {len(tasks)} задач", 0)

    def _change_status(self, status, progress=0):
        """Обновляет статус воркера в словаре и прогресс-баре."""
        self.__class__.workers_info[self.proxy] = status
        bar = self.__class__.progress_bars.get(self.proxy)
        if bar:
            bar.set_description_str(f"{self.proxy.ljust(50)} [{status}]")
            bar.n = progress
            bar.refresh()

    @classmethod
    def close_all_bars(cls):
        """Закрывает все прогресс-бары (вызывать при завершении)."""
        for bar in cls.progress_bars.values():
            bar.close()