#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Модуль для авторизации в Steam через библиотеку steampy.
Использует данные из папки accounts/<username>/data.json и прокси из файла proxy_for_main.txt.
Логирование выполняется через БД с помощью log_sync из utils.
"""

import os
import json
import pickle
import traceback
from pathlib import Path

from steampy.client import SteamClient

# Импортируем синхронное логирование из utils
from src.utils import log_sync


class Bot:
    """Класс для управления сессией Steam и логином."""

    def __init__(self, username: str):
        """
        Инициализирует бота для указанного пользователя.
        Создаёт папку accounts/<username>, если её нет.
        """
        self.username = username
        self.steam_client = None

        # Путь к папке accounts
        accounts_dir = Path("accounts")
        if not accounts_dir.exists():
            accounts_dir.mkdir()
            log_sync("Создана папка accounts", "INFO", "steam_logger")

        # Путь к папке конкретного пользователя
        user_dir = accounts_dir / username
        try:
            user_dir.mkdir(exist_ok=True)
            log_sync(f"Папка {user_dir} найдена/создана", "DEBUG", "steam_logger")
        except OSError as e:
            log_sync(f"Ошибка при создании папки {user_dir}: {e}", "ERROR", "steam_logger")
            raise

    def _load_proxies(self) -> dict | None:
        """
        Загружает прокси из файла proxy_for_main.txt.
        Ожидается формат: ip:port:user:pass (без протокола).
        Возвращает словарь для steampy или None, если прокси не нужны.
        """
        proxy_file = Path("../test/proxy_for_main.txt")  # Относительный путь, как в оригинале
        if not proxy_file.exists():
            log_sync("Файл proxy_for_main.txt не найден, работа без прокси", "WARNING", "steam_logger")
            return None

        try:
            with open(proxy_file, "r", encoding="utf-8") as f:
                content = f.read().strip()
                if not content:
                    log_sync("Файл proxy_for_main.txt пуст, работа без прокси", "WARNING", "steam_logger")
                    return None

                # Ожидается формат ip:port:user:pass
                parts = content.split(":")
                if len(parts) != 4:
                    log_sync(f"Неверный формат прокси: {content}. Ожидается ip:port:user:pass", "ERROR", "steam_logger")
                    return None

                proxy_url = f"http://{parts[2]}:{parts[3]}@{parts[0]}:{parts[1]}"
                proxies = {
                    'http': proxy_url,
                    'https': proxy_url
                }
                log_sync(f"Использую прокси: {proxy_url}", "INFO", "steam_logger")
                return proxies
        except Exception as e:
            log_sync(f"Ошибка при загрузке прокси: {traceback.format_exc()}", "ERROR", "steam_logger")
            return None

    def login(self) -> SteamClient:
        """
        Выполняет вход в Steam.
        Если есть сохранённая сессия (cookies.pkl), пробует восстановить её.
        Иначе создаёт новый клиент и выполняет логин.
        Возвращает экземпляр SteamClient.
        """
        user_dir = Path("accounts") / self.username
        data_file = user_dir / "data.json"
        cookies_file = user_dir / "cookies.pkl"

        # Загружаем прокси
        proxies = self._load_proxies()

        # Если файл data.json не существует, создаём шаблон
        if not data_file.exists():
            log_sync(f"Файл {data_file} не найден. Создаю шаблон...", "INFO", "steam_logger")
            template = {
                "login": "",
                "password": "",
                "shared_secret": "",
                "identity_secret": "",
                "steamid": "",
                "web_api": ""
            }
            try:
                with open(data_file, "w", encoding="utf-8") as f:
                    json.dump(template, f, indent=4)
                log_sync(f"Шаблон {data_file} создан. Заполните его данными.", "WARNING", "steam_logger")
            except Exception as e:
                log_sync(f"Не удалось создать шаблон {data_file}: {e}", "ERROR", "steam_logger")
            raise FileNotFoundError(f"Файл {data_file} не заполнен")
        else:
            log_sync(f"Файл {data_file} найден, загружаю данные...", "DEBUG", "steam_logger")
            with open(data_file, "r", encoding="utf-8") as f:
                steam_data = json.load(f)

            api_key = steam_data.get("web_api")
            username = steam_data.get("login")
            password = steam_data.get("password")

            if not api_key or not username or not password:
                log_sync(f"В файле {data_file} отсутствуют обязательные поля (web_api, login, password)", "ERROR", "steam_logger")
                raise ValueError("Неполные данные в data.json")

            # Создаём клиент Steam
            steam_client = SteamClient(api_key, username, password, steam_guard=str(data_file), proxies=proxies)
            self.steam_client = steam_client

        # Пытаемся загрузить сохранённую сессию
        if cookies_file.exists():
            try:
                with open(cookies_file, "rb") as f:
                    saved_client = pickle.load(f)
                if saved_client.is_session_alive():
                    log_sync("Сессия восстановлена из cookies.pkl", "INFO", "steam_logger")
                    self.steam_client = saved_client
                    return self.steam_client
                else:
                    log_sync("Сохранённая сессия неактивна, выполняю новый вход", "INFO", "steam_logger")
            except Exception as e:
                log_sync(f"Ошибка при загрузке cookies.pkl: {traceback.format_exc()}", "ERROR", "steam_logger")
        else:
            log_sync("Файл cookies.pkl не найден, выполняю новый вход", "INFO", "steam_logger")

        # Выполняем новый вход
        try:
            self.steam_client.login()
        except Exception as e:
            log_sync(f"Ошибка при входе в Steam: {traceback.format_exc()}", "ERROR", "steam_logger")
            raise

        if self.steam_client.is_session_alive():
            log_sync("Вход выполнен успешно, сохраняю сессию...", "INFO", "steam_logger")
            try:
                with open(cookies_file, "wb") as f:
                    pickle.dump(self.steam_client, f)
                log_sync("Сессия сохранена в cookies.pkl", "INFO", "steam_logger")
            except Exception as e:
                log_sync(f"Не удалось сохранить сессию: {traceback.format_exc()}", "ERROR", "steam_logger")
            return self.steam_client
        else:
            log_sync("Не удалось войти в Steam (сессия неактивна после login)", "ERROR", "steam_logger")
            raise RuntimeError("Login failed")