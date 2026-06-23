# GophProfile

Микросервис для управления аватарками пользователей.

## Стек
- Go (Chi)
- PostgreSQL
- MinIO (S3)
- Kafka
- OpenTelemetry (traces)
- Prometheus (metrics)
- Jaeger (tracing UI)
- Grafana (dashboards)
- Loki + Promtail (logs)
- Alertmanager (alerts)
- Docker / Docker Compose

## Быстрый старт

```bash
# Запустить всё окружение
docker compose up --build -d

# Проверить health
curl http://localhost:8080/health
# Проверить метрики
curl http://localhost:8080/metrics

# Загрузить аватарку
curl -X POST http://localhost:8080/api/v1/avatars \
  -H "X-User-ID: user1" \
  -F "image=@photo.jpg"

# Веб-интерфейс
http://localhost:8080/web/upload
```

## Наблюдаемость

После запуска `docker compose up --build -d` доступны:

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (логин/пароль: `admin` / `admin`)
- **Jaeger**: http://localhost:16686
- **Alertmanager**: http://localhost:9093
- **Loki**: http://localhost:3100 (обычно используется через Grafana)

### Что уже настроено

- HTTP RED-метрики (`avatars_http_requests_total`, `avatars_http_errors_total`, `avatars_http_request_duration_seconds`)
- Бизнес-метрики (`avatars_uploads_total`, `avatars_upload_duration_seconds`, `avatars_storage_bytes`)
- Инфраструктурные метрики (`avatars_db_connections`, `avatars_queue_depth`, `avatars_kafka_messages_total`)
- Распределённый трейсинг для HTTP, PostgreSQL, MinIO, Kafka producer/consumer и worker
- Context propagation через Kafka headers (`traceparent` / `tracestate`)
- Структурированные JSON-логи на `slog` с `trace_id`/`span_id`
- Grafana dashboard: `GophProfile - Service Overview`
- Alert rules в `deploy/prometheus/alerts.yml`:
  - `HighErrorRate`
  - `HighResponseTime`

## API

| Метод | Путь | Описание |
|--------|------|----------|
| POST | /api/v1/avatars | Загрузка аватарки |
| GET | /api/v1/avatars/{id} | Получение изображения |
| GET | /api/v1/avatars/{id}/metadata | Метаданные |
| DELETE | /api/v1/avatars/{id} | Удаление |
| GET | /api/v1/users/{id}/avatar | Аватарка пользователя |
| GET | /api/v1/users/{id}/avatars | Список аватарок |
| DELETE | /api/v1/users/{id}/avatar | Удаление всех |
| GET | /health | Healthcheck |

## Тесты

```bash
go test ./internal/... -v
```