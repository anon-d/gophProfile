package domain

import "errors"

// Sentinel-ошибки бизнес-логики.
var (
	ErrNotFound          = errors.New("not found")
	ErrForbidden         = errors.New("forbidden")
	ErrFileTooLarge      = errors.New("file too large")
	ErrUnsupportedFormat = errors.New("unsupported file format")
	ErrAlreadyDeleted    = errors.New("already deleted")
)
