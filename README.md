# GophProfile

Микросервис для управления аватарками пользователей.

## Стек
- Go (Chi)
- PostgreSQL
- MinIO (S3)
- Kafka
- Docker / Docker Compose

## Быстрый старт

```bash
# Запустить всё окружение
docker compose up --build -d

# Проверить health
curl http://localhost:8080/health

# Загрузить аватарку
curl -X POST http://localhost:8080/api/v1/avatars \
  -H "X-User-ID: user1" \
  -F "image=@photo.jpg"

# Веб-интерфейс
http://localhost:8080/web/upload
```

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