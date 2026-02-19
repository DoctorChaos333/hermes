#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Сканер предметов на торговой площадке Steam.
Запускается отдельно от основного бота и обновляет данные о предметах в БД.
Использует асинхронные запросы через aiohttp и пул соединений с БД.
"""

import asyncio
import json
import time
import traceback
from pathlib import Path

from src.market import MarketWorker
from src.steam_logger import Bot
from src.utils import get_old_items, get_settings, log_async
from src.async_db import Storage

# Глобальный event loop для всего модуля
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)


def load_config():
    """Загружает данные аккаунта и прокси из файлов."""
    data_path = Path("data.json")
    if not data_path.exists():
        raise FileNotFoundError("Файл data.json не найден")
    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)

    proxies = []
    proxies_path = Path("proxies.txt")
    if proxies_path.exists():
        with open(proxies_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(":")
                    if len(parts) == 4:
                        proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                        proxies.append(proxy_url)
    else:
        loop.run_until_complete(log_async("Файл proxies.txt не найден, работа без прокси", "WARNING", "scanner"))

    return data, proxies


async def scan(links=None):
    """
    Запускает сканирование предметов.
    Если links не передан, получает список устаревших предметов из БД.
    """
    if links is None:
        settings = await get_settings(data['login'])
        links = await get_old_items(settings)

    await log_async(f"Всего ссылок для сканирования: {len(links)}", "INFO", "scanner")

    if not links:
        await log_async("Нет предметов для сканирования", "INFO", "scanner")
        return

    # Логинимся в Steam (синхронно, получаем куки)
    bot = Bot(data['login'])
    steam_client = bot.login()
    cookies = steam_client._session.cookies
    cookies_dict = {cookie.name: cookie.value for cookie in cookies}

    # Создаём воркеров по числу прокси (не больше, чем ссылок)
    workers = []
    for i in range(min(len(proxies), len(links))):
        proxy = proxies[i]
        worker = MarketWorker(proxy, cookies, cookies_dict, login=data['login'])
        workers.append(worker)

    # Распределяем ссылки между воркерами
    while links:
        for worker in workers:
            if links:
                item = links.pop(0)
                worker.add_tasks([item])

    # Запускаем всех воркеров параллельно
    tasks = [asyncio.create_task(worker.start_working()) for worker in workers]
    await asyncio.gather(*tasks)


# Загружаем конфигурацию один раз перед циклом
data, proxies = load_config()

# Бесконечный цикл сканирования
while True:
    try:
        # Инициализируем пул БД (если ещё не инициализирован)
        loop.run_until_complete(Storage.init_pool())
        loop.run_until_complete(log_async("Запуск сканирования...", "INFO", "scanner"))

        # Запускаем сканирование
        loop.run_until_complete(scan())

    except KeyboardInterrupt:
        loop.run_until_complete(log_async("Сканер остановлен пользователем", "INFO", "scanner"))
        break
    except Exception as e:
        loop.run_until_complete(log_async(f"Критическая ошибка в сканере: {traceback.format_exc()}", "CRITICAL", "scanner"))
        # Пауза перед повторной попыткой
        time.sleep(60)
        continue

    # Ожидание 1 час до следующего сканирования
    loop.run_until_complete(log_async("Сканирование завершено. Ожидание 1 час...", "INFO", "scanner"))
    time.sleep(3600)

# Закрываем пул БД и event loop при завершении
loop.run_until_complete(Storage.close_pool())
loop.close()