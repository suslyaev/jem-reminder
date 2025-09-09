#!/bin/bash

# Скрипт деплоя JEM Reminder на сервер
# Использование: ./deploy.sh

set -e

# Настройки берём из deploy/.env (если есть) или из переменных окружения
if [ -f "./deploy/.env" ]; then
  set -a
  . ./deploy/.env
  set +a
fi

# Обязательные переменные
SERVER="${DEPLOY_SERVER:?Set DEPLOY_SERVER in deploy/.env (например, root@1.2.3.4)}"
APP_DIR="${DEPLOY_APP_DIR:-/opt/jem-reminder}"
REPO_URL="${DEPLOY_REPO_URL:?Set DEPLOY_REPO_URL in deploy/.env}"
DOMAIN="${DEPLOY_DOMAIN:?Set DEPLOY_DOMAIN in deploy/.env (например, example.com)}"

ssh "$SERVER" "DOMAIN='$DOMAIN' APP_DIR='$APP_DIR' REPO_URL='$REPO_URL' bash -s" <<'REMOTE'
set -e

apt update -y && apt install -y python3 python3-venv nginx git certbot python3-certbot-nginx

rm -rf "$APP_DIR"
mkdir -p "$APP_DIR"
cd "$APP_DIR"
git clone "$REPO_URL" .

python3 -m venv env
source env/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

python database/init_db.py

cp deploy/jem-reminder-bot.service /etc/systemd/system/
cp deploy/jem-reminder-web.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable jem-reminder-bot jem-reminder-web
systemctl restart jem-reminder-bot jem-reminder-web

# Подставляем домен в nginx.conf и применяем
sed "s/your-domain.tld/$DOMAIN/g" deploy/nginx.conf > /etc/nginx/sites-available/jem-reminder
ln -sf /etc/nginx/sites-available/jem-reminder /etc/nginx/sites-enabled/
nginx -t && systemctl reload nginx

echo "OK"
REMOTE


