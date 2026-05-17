FROM docker.io/apache/airflow:2.9.3 AS airflow-base

COPY requirements.txt /requirements.txt
COPY data/neo.csv /opt/airflow/data/neo.csv
ENV _PIP_ADDITIONAL_REQUIREMENTS="-r /requirements.txt"

FROM node:20-alpine AS frontend-builder
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install
COPY frontend .
RUN npm run build

FROM node:20-alpine AS frontend
WORKDIR /app
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm install --production
COPY --from=frontend-builder /app/dist ./dist
COPY frontend/server.js ./
EXPOSE 4173
CMD ["node", "server.js"]

FROM airflow-base