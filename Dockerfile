FROM node:20-bookworm-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV PORT=8080
ENV DB_PATH=/data/radar.db
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx python3 python3-venv gettext-base supervisor ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
RUN pip install --no-cache-dir -r backend/requirements.txt

COPY deploy/nginx.conf.template /etc/nginx/nginx.conf.template
COPY deploy/supervisord.conf /etc/supervisor/conf.d/radar.conf
RUN mkdir -p /data /run/nginx /var/log/supervisor
EXPOSE 8080
CMD ["/bin/sh", "-c", "export PORT=${PORT:-8080}; envsubst '${PORT}' < /etc/nginx/nginx.conf.template > /etc/nginx/nginx.conf; mkdir -p /data; exec /usr/bin/supervisord -n -c /etc/supervisor/supervisord.conf"]
