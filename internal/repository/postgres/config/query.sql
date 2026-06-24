-- Создание аватарки
-- name: CreateAvatar :one
INSERT INTO avatars (user_id, file_name, mime_type, size_bytes, s3_key, upload_status, processing_status)
VALUES ($1, $2, $3, $4, $5, 'done', 'pending')
RETURNING id, user_id, file_name, mime_type, size_bytes, s3_key, thumbnail_s3_keys,
           upload_status, processing_status, created_at, updated_at;

-- Получение аватарки по ID (без учёта удалённых)
-- name: GetAvatarByID :one
SELECT id, user_id, file_name, mime_type, size_bytes, s3_key, thumbnail_s3_keys,
       upload_status, processing_status, created_at, updated_at
FROM avatars
WHERE id = $1 AND deleted_at IS NULL;

-- Получение последней аватарки пользователя
-- name: GetAvatarByUserID :one
SELECT id, user_id, file_name, mime_type, size_bytes, s3_key, thumbnail_s3_keys,
       upload_status, processing_status, created_at, updated_at
FROM avatars
WHERE user_id = $1 AND deleted_at IS NULL
ORDER BY created_at DESC
LIMIT 1;

-- Список аватарок пользователя
-- name: ListAvatarsByUserID :many
SELECT id, user_id, file_name, mime_type, size_bytes, s3_key, thumbnail_s3_keys,
       upload_status, processing_status, created_at, updated_at
FROM avatars
WHERE user_id = $1 AND deleted_at IS NULL
ORDER BY created_at DESC;

-- Мягкое удаление аватарки (с проверкой владельца)
-- name: SoftDeleteAvatar :execresult
UPDATE avatars
SET deleted_at = NOW(), updated_at = NOW()
WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL;

-- Мягкое удаление аватарки по user_id (все аватарки пользователя)
-- name: SoftDeleteAvatarByUserID :execresult
UPDATE avatars
SET deleted_at = NOW(), updated_at = NOW()
WHERE user_id = $1 AND deleted_at IS NULL;

-- Обновление статуса обработки и миниатюр
-- name: UpdateProcessingStatus :exec
UPDATE avatars
SET processing_status = $2, thumbnail_s3_keys = $3, updated_at = NOW()
WHERE id = $1;

-- Обновление upload-статуса
-- name: UpdateUploadStatus :exec
UPDATE avatars
SET upload_status = $2, updated_at = NOW()
WHERE id = $1;

-- Обновление S3-ключа после загрузки
-- name: UpdateS3Key :exec
UPDATE avatars
SET s3_key = $2, updated_at = NOW()
WHERE id = $1;
