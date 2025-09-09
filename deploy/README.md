# Деплой JEM Reminder

Краткие шаги:

1) На сервере подготовьте окружение и код:
```
apt update -y && apt install -y python3 python3-venv nginx git certbot python3-certbot-nginx
mkdir -p /opt/jem-reminder && cd /opt/jem-reminder
git clone git@github.com:your-user/your-repo.git .
python3 -m venv env && source env/bin/activate
pip install --upgrade pip && pip install -r requirements.txt
python database/init_db.py
```

2) Создайте .env (секреты — только на сервере):
```
cp deploy/env.production.example .env
nano .env
```

Заполните значения:
- BOT_TOKEN
- SUPERADMIN_ID
- TEST_TELEGRAM_ID (опционально)

3) Systemd сервисы:
```
cp deploy/jem-reminder-bot.service /etc/systemd/system/
cp deploy/jem-reminder-web.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable jem-reminder-bot jem-reminder-web
systemctl restart jem-reminder-bot jem-reminder-web
```

4) Nginx и SSL:
```
cp deploy/nginx.conf /etc/nginx/sites-available/jem-reminder
ln -sf /etc/nginx/sites-available/jem-reminder /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx
certbot --nginx -d your-domain.tld -d www.your-domain.tld

## Переменные деплоя (deploy/.env)

Создайте файл `deploy/.env` локально (не коммитить) или экспортируйте переменные окружения:

```
# сервер SSH (с пользователем)
DEPLOY_SERVER=root@1.2.3.4

# путь к приложению на сервере
DEPLOY_APP_DIR=/opt/jem-reminder

# репозиторий для клонирования
DEPLOY_REPO_URL=git@github.com:your-user/your-repo.git

# домен для nginx
DEPLOY_DOMAIN=your-domain.tld
```
```

5) Обновления:
```
cd /opt/jem-reminder
git pull
source env/bin/activate
pip install -r requirements.txt
systemctl restart jem-reminder-bot jem-reminder-web
```

.gitignore:
```
# Секреты
.env
*.env
```
