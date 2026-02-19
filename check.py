#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт проверки целостности проекта и готовности к запуску.
Проверяет наличие файлов, зависимостей, локальной версии steampy, подключение к БД.
"""

import os
import sys
import importlib
import subprocess
from pathlib import Path

# Цвета для вывода (опционально)
class Colors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'

def print_ok(msg):
    print(f"{Colors.OKGREEN}[OK]{Colors.ENDC} {msg}")

def print_warning(msg):
    print(f"{Colors.WARNING}[WARNING]{Colors.ENDC} {msg}")

def print_error(msg):
    print(f"{Colors.FAIL}[ERROR]{Colors.ENDC} {msg}")

def print_info(msg):
    print(f"{Colors.OKBLUE}[INFO]{Colors.ENDC} {msg}")

def check_file_exists(filepath, create_if_missing=False, template_content=""):
    """Проверяет существование файла. Если create_if_missing=True, создаёт пустой или с шаблоном."""
    path = Path(filepath)
    if path.exists():
        print_ok(f"Файл {filepath} найден.")
        return True
    else:
        if create_if_missing:
            try:
                with open(path, 'w', encoding='utf-8') as f:
                    f.write(template_content)
                print_warning(f"Файл {filepath} отсутствовал, создан пустой шаблон. Заполните его.")
                return True
            except Exception as e:
                print_error(f"Не удалось создать файл {filepath}: {e}")
                return False
        else:
            print_error(f"Файл {filepath} не найден.")
            return False

def check_dir_exists(dirpath, create_if_missing=False):
    """Проверяет существование папки."""
    path = Path(dirpath)
    if path.exists() and path.is_dir():
        print_ok(f"Папка {dirpath} найдена.")
        return True
    else:
        if create_if_missing:
            try:
                path.mkdir(parents=True, exist_ok=True)
                print_warning(f"Папка {dirpath} отсутствовала, создана.")
                return True
            except Exception as e:
                print_error(f"Не удалось создать папку {dirpath}: {e}")
                return False
        else:
            print_error(f"Папка {dirpath} не найдена.")
            return False

def check_steampy_local():
    """Проверяет, что используется локальная версия steampy, а не установленная глобально."""
    try:
        import steampy
        # Получаем путь к модулю
        module_path = Path(steampy.__file__).resolve()
        # Предположим, что локальная версия находится в папке проекта (например, ./steampy или ./src/steampy)
        project_root = Path.cwd().resolve()
        if project_root in module_path.parents:
            print_ok(f"Используется локальная версия steampy: {module_path}")
            return True
        else:
            print_warning(f"Импортируется глобальная версия steampy: {module_path}. Убедитесь, что используется локальная модифицированная версия.")
            # Можно дополнительно проверить наличие папки steampy в проекте
            local_steampy_dir = project_root / "steampy"
            if local_steampy_dir.exists():
                print_info("Локальная папка steampy найдена, но не импортируется. Проверьте PYTHONPATH или структуру импортов.")
            return False
    except ImportError:
        print_error("Библиотека steampy не импортируется. Убедитесь, что локальная версия находится в проекте и доступна для импорта.")
        return False

def check_dependencies(requirements_file="requirements.txt"):
    """Проверяет наличие необходимых библиотек, при отсутствии предлагает установить."""
    required_packages = []
    if Path(requirements_file).exists():
        with open(requirements_file, 'r', encoding='utf-8') as f:
            required_packages = [line.strip().split('==')[0] for line in f if line.strip() and not line.startswith('#')]
    else:
        # Базовый список, если нет requirements.txt
        required_packages = [
            'aiohttp', 'aiomysql', 'fake_useragent', 'tqdm', 'bs4',
            'python-dotenv', 'steampy'  # steampy будет проверен отдельно
        ]
        print_warning(f"Файл {requirements_file} не найден, используем встроенный список зависимостей.")

    missing = []
    for pkg in required_packages:
        if pkg == 'steampy':
            # steampy проверяем отдельно, так как он локальный
            continue
        try:
            importlib.import_module(pkg)
            print_ok(f"Библиотека {pkg} найдена.")
        except ImportError:
            missing.append(pkg)
            print_warning(f"Библиотека {pkg} отсутствует.")

    if missing:
        print_info("Отсутствуют следующие библиотеки: " + ", ".join(missing))
        answer = input("Установить их с помощью pip? (y/n): ").strip().lower()
        if answer == 'y':
            for pkg in missing:
                try:
                    subprocess.check_call([sys.executable, "-m", "pip", "install", pkg])
                    print_ok(f"Установлена {pkg}")
                except subprocess.CalledProcessError as e:
                    print_error(f"Ошибка при установке {pkg}: {e}")
                    return False
        else:
            print_warning("Пропускаем установку. Работа программы может быть нестабильной.")
    return True

def check_env_file():
    """Проверяет наличие .env и наличие в нём необходимых переменных."""
    env_path = Path(".env")
    if not env_path.exists():
        # Попробуем создать шаблон .env из .env.example, если есть
        example_path = Path(".env.example")
        if example_path.exists():
            import shutil
            shutil.copy(example_path, env_path)
            print_warning("Файл .env отсутствовал, создан из .env.example. Заполните его.")
        else:
            # Создадим пустой шаблон
            template = """# Настройки базы данных
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASSWORD=
DB_DATABASE=steam_bot

# Другие настройки (если есть)
"""
            with open(env_path, 'w', encoding='utf-8') as f:
                f.write(template)
            print_warning("Файл .env отсутствовал, создан шаблон. Заполните его.")
    else:
        print_ok("Файл .env найден.")

    # Проверим наличие основных переменных
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        print_error("Не удалось импортировать dotenv для проверки .env. Установите python-dotenv.")
        return False

    required_vars = ['DB_HOST', 'DB_PORT', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print_warning(f"В .env отсутствуют переменные: {', '.join(missing_vars)}. Добавьте их.")
        return False
    else:
        print_ok("Все необходимые переменные в .env присутствуют.")
        return True

async def check_db_connection():
    """Проверяет подключение к БД, используя класс Storage."""
    try:
        from src.async_db import Storage
        await Storage.init_pool()
        async with Storage() as db:
            # Простой запрос для проверки
            result = await db.fetchone("SELECT 1")
            if result:
                print_ok("Подключение к базе данных успешно.")
                # Дополнительно можно проверить наличие таблиц (необязательно)
                tables = await db.fetchall("SHOW TABLES")
                if tables:
                    print_ok(f"Найдено таблиц: {len(tables)}.")
                else:
                    print_warning("База данных пуста (нет таблиц). Возможно, требуется инициализация.")
                return True
            else:
                print_error("Не удалось выполнить тестовый запрос к БД.")
                return False
    except Exception as e:
        print_error(f"Ошибка подключения к БД: {e}")
        return False
    finally:
        await Storage.close_pool()

def main():
    print_info("=== Проверка целостности проекта ===")

    # 1. Проверка структуры папок
    check_dir_exists("accounts", create_if_missing=True)
    check_dir_exists("src", create_if_missing=False)  # src должна быть
    check_dir_exists("logs", create_if_missing=True)  # для логов, если используем файловые логи

    # 2. Проверка важных файлов
    check_file_exists("data.json", create_if_missing=True, template_content="""{
  "login":"",
  "password": "",
  "shared_secret":"",
  "identity_secret":"",
  "steamid":"",
  "web_api":""
}""")
    check_file_exists("proxies.txt", create_if_missing=True, template_content="host:port:log:pass")
    check_file_exists(".env", create_if_missing=False)  # проверим отдельно с содержимым
    check_file_exists("requirements.txt", create_if_missing=False)


    # 3. Проверка .env и переменных
    env_ok = check_env_file()

    # 4. Проверка зависимостей
    deps_ok = check_dependencies()

    # 5. Проверка локальной steampy
    steampy_ok = check_steampy_local()

    # 6. Проверка подключения к БД (только если .env ок)
    db_ok = False
    if env_ok:
        import asyncio
        db_ok = asyncio.run(check_db_connection())

    # Итог
    print_info("=== Результаты проверки ===")
    if not steampy_ok:
        print_error("Проблема с локальной версией steampy. Убедитесь, что модифицированная версия находится в проекте и доступна для импорта.")
    if not env_ok:
        print_error("Проблема с файлом .env. Заполните его корректными данными.")
    if not db_ok:
        print_error("Проблема с подключением к базе данных. Проверьте настройки в .env и доступность MySQL.")
    if not deps_ok:
        print_error("Проблема с зависимостями. Установите недостающие библиотеки.")

    if steampy_ok and env_ok and db_ok and deps_ok:
        print_ok("Все проверки пройдены успешно. Проект готов к запуску.")
    else:
        print_warning("Некоторые проверки не пройдены. Исправьте ошибки перед запуском.")

if __name__ == "__main__":
    main()