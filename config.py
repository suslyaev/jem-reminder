import os
from typing import Optional

def load_config():
    """Загружает конфигурацию из файла .env"""
    config = {}
    
    # Пытаемся загрузить из config.env
    if os.path.exists('.env'):
        with open('.env', 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key.strip()] = value.strip()
    
    # Проверяем обязательные параметры
    required_keys = ['BOT_TOKEN', 'SUPERADMIN_ID']
    missing_keys = [key for key in required_keys if key not in config]
    
    if missing_keys:
        raise ValueError(f"Отсутствуют обязательные параметры в .env: {', '.join(missing_keys)}")
    
    return config

# Загружаем конфигурацию при импорте модуля
try:
    CONFIG = load_config()
    BOT_TOKEN = CONFIG['BOT_TOKEN']
    # Необязательное имя бота для генерации ссылок добавления в группы
    # Если в .env нет BOT_NAME, пробуем из переменной окружения GROUP_NAME (для обратной совместимости с web/app.py)
    BOT_NAME: Optional[str] = CONFIG.get('BOT_NAME') or os.getenv('GROUP_NAME')
    
    # Поддержка как одного ID, так и массива ID суперадминов
    superadmin_str = CONFIG['SUPERADMIN_ID']
    if ',' in superadmin_str:
        # Массив ID через запятую
        SUPERADMIN_IDS = [int(x.strip()) for x in superadmin_str.split(',') if x.strip()]
    else:
        # Один ID
        SUPERADMIN_IDS = [int(superadmin_str)]
    
    # Для обратной совместимости оставляем SUPERADMIN_ID как первый элемент
    SUPERADMIN_ID = SUPERADMIN_IDS[0] if SUPERADMIN_IDS else None
    
except Exception as e:
    print(f"Ошибка загрузки конфигурации: {e}")
    print("Убедитесь, что файл .env существует и содержит необходимые параметры")
    exit(1)
