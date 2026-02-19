#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Основной файл бота для перепродажи предметов на торговой площадке Steam.
Работает синхронно, но вызывает асинхронные функции БД через единый event loop.
Все логи пишутся в базу данных (таблица logs).
"""

import asyncio
import json
import time
import traceback
from datetime import datetime, timedelta
from pathlib import Path

# Импорты из проекта
from src.steam_logger import Bot
from src.utils import (
    get_settings,
    get_orders,
    orders_update_needed,
    update_orders,
    Orders,
    sell_items,
    set_loop,
    log_sync
)

# Импортируем класс Storage для работы с БД и функцией log
from src.async_db import Storage

# Глобальный event loop для вызова асинхронных функций БД
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
set_loop(loop)



def load_config():
    """
    Загружает конфигурационные данные из файлов data.json и proxies.txt.
    Возвращает кортеж (data, proxies).
    """
    # Загрузка данных аккаунта
    data_path = Path("data.json")
    if not data_path.exists():
        raise FileNotFoundError("Файл data.json не найден")
    with open(data_path, encoding="utf-8") as f:
        data = json.load(f)

    # Загрузка прокси
    proxies_path = Path("proxies.txt")
    proxies = []
    if proxies_path.exists():
        with open(proxies_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    parts = line.split(":")
                    # Ожидаемый формат: ip:port:user:pass
                    if len(parts) == 4:
                        proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                        proxies.append(proxy_url)
    else:
        log_sync("Файл proxies.txt не найден, работа без прокси", "WARNING", "main")

    return data, proxies


def main():
    log_sync("Запуск бота...", "INFO", "main")
    data, proxies = load_config()
    log_sync(f"Аккаунт: {data['login']}, прокси: {len(proxies)}", "INFO", "main")

    # Получаем настройки из БД (первый асинхронный вызов)
    try:
        settings = loop.run_until_complete(get_settings(data["login"]))
        log_sync("Настройки получены", "INFO", "main")
    except Exception as e:
        log_sync(f"Не удалось получить настройки из БД: {e}", "ERROR", "main")
        log_sync(traceback.format_exc(), "ERROR", "main")
        return  # Завершаем, так как без настроек работать нельзя

    # Бесконечный цикл работы
    while True:
        try:
            # Логинимся в Steam (синхронно)
            bot = Bot(data["login"])
            steam_client = bot.login()
            log_sync("Успешный вход в Steam", "INFO", "main")

            # Проверка актуальности ордеров в БД
            try:
                is_update_needed = loop.run_until_complete(orders_update_needed(settings))
                if is_update_needed:
                    log_sync("Ордера устарели, выполняю обновление...", "INFO", "main")
                    orders_data = steam_client.market.get_my_market_listings()
                    loop.run_until_complete(update_orders(orders_data))
                    log_sync("Ордера обновлены", "INFO", "main")
            except Exception as e:
                log_sync(f"Ошибка при обновлении ордеров: {e}", "ERROR", "main")
                log_sync(traceback.format_exc(), "ERROR", "main")

            # Получаем текущие ордера из БД
            try:
                orders_db = loop.run_until_complete(get_orders())
                log_sync(
                    f"Получено ордеров: покупка: {len(orders_db.get('buy_orders', []))}, "
                    f"продажа: {len(orders_db.get('sell_listings', []))}",
                    "INFO", "main"
                )
            except Exception as e:
                log_sync(f"Ошибка при получении ордеров из БД: {e}", "ERROR", "main")
                orders_db = {"buy_orders": [], "sell_listings": []}

            # Создаём объект Orders
            orders = Orders(orders_db, settings)

            # Отмена устаревших ордеров на продажу
            try:
                orders.cancel_sell_listings(steam_client)
            except Exception as e:
                log_sync(f"Ошибка при отмене ордеров на продажу: {e}", "ERROR", "main")
                log_sync(traceback.format_exc(), "ERROR", "main")

            # Продажа предметов из инвентаря
            try:
                sell_items(steam_client)
            except Exception as e:
                log_sync(f"Ошибка при продаже предметов: {e}", "ERROR", "main")
                log_sync(traceback.format_exc(), "ERROR", "main")

            # Выставление ордеров на покупку
            try:
                orders.set_buy_orders(settings, steam_client)
                log_sync(f"[{datetime.now()}] Проверка ордеров завершена", "INFO", "main")
            except Exception as e:
                log_sync(f"Ошибка при простановке ордеров: {e}", "ERROR", "main")
                log_sync(traceback.format_exc(), "ERROR", "main")

        except Exception as e:
            log_sync(f"Критическая ошибка в основном цикле: {e}", "CRITICAL", "main")
            log_sync(traceback.format_exc(), "CRITICAL", "main")
            # Пауза 60 секунд перед следующей попыткой
            log_sync("Пауза 60 секунд...", "INFO", "main")
            time.sleep(60)
            continue

        # Ожидание 1 час до следующего цикла
        next_run = datetime.now() + timedelta(hours=1)
        log_sync(f"Цикл завершён. Следующий запуск в {next_run.strftime('%Y-%m-%d %H:%M:%S')}", "INFO", "main")
        time.sleep(3600)
        log_sync("Новый цикл начинается...", "INFO", "main")


if __name__ == "__main__":
    try:
        # Инициализируем пул соединений с БД (один раз при старте)
        loop.run_until_complete(Storage.init_pool())
        log_sync("Пул соединений с БД инициализирован", "INFO", "main")

        # Запускаем основную функцию
        main()
    except KeyboardInterrupt:
        log_sync("Бот остановлен пользователем", "INFO", "main")
    except Exception as e:
        # В случае критической ошибки пытаемся записать лог
        try:
            log_sync(f"Необработанная ошибка: {e}", "CRITICAL", "main")
            log_sync(traceback.format_exc(), "CRITICAL", "main")
        except:
            pass
        print(f"Критическая ошибка: {e}")
    finally:
        # Закрываем пул соединений и event loop
        try:
            loop.run_until_complete(Storage.close_pool())
            log_sync("Пул соединений с БД закрыт", "INFO", "main")
        except:
            pass
        loop.close()
        print("Event loop закрыт")