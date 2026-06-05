package postgres

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestUuidToPgtype(t *testing.T) {
	id := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	u := uuidToPgtype(id)
	if !u.Valid {
		t.Error("expected valid UUID")
	}
}

func TestUuidToString(t *testing.T) {
	id := "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"
	u := uuidToPgtype(id)
	result := uuidToString(u)

	if result != id {
		t.Errorf("expected %s, got %s", id, result)
	}
}

func TestUuidToString_Invalid(t *testing.T) {
	u := pgtype.UUID{Valid: false}
	result := uuidToString(u)
	if result != "" {
		t.Errorf("expected empty string, got %s", result)
	}
}

func TestParseThumbnails_Empty(t *testing.T) {
	result := parseThumbnails(nil)
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}

	result = parseThumbnails([]byte{})
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestParseThumbnails_Valid(t *testing.T) {
	data := []byte(`{"100x100":"thumbnails/abc/100x100.jpg","300x300":"thumbnails/abc/300x300.jpg"}`)
	result := parseThumbnails(data)

	if len(result) != 2 {
		t.Fatalf("expected 2, got %d", len(result))
	}
	if result["100x100"] != "thumbnails/abc/100x100.jpg" {
		t.Errorf("unexpected value: %s", result["100x100"])
	}
}
