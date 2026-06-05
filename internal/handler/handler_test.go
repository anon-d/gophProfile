package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWriteJSON(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]string{"key": "value"}

	writeJSON(w, http.StatusOK, data)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}

	var result map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected value, got %s", result["key"])
	}
}

func TestWriteJSON_StatusCreated(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]interface{}{
		"id":     "test-id",
		"status": "processing",
	}

	writeJSON(w, http.StatusCreated, data)

	if w.Code != http.StatusCreated {
		t.Errorf("expected 201, got %d", w.Code)
	}
}

func TestWriteJSON_Error(t *testing.T) {
	w := httptest.NewRecorder()
	data := map[string]string{"error": "not found"}

	writeJSON(w, http.StatusNotFound, data)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}

	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["error"] != "not found" {
		t.Errorf("expected 'not found', got %s", result["error"])
	}
}

func TestUpload_MissingUserID(t *testing.T) {
	h := &AvatarHandler{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", nil)
	w := httptest.NewRecorder()

	h.Upload(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var result map[string]string
	json.NewDecoder(w.Body).Decode(&result)
	if result["error"] != "X-User-ID header is required" {
		t.Errorf("unexpected error: %s", result["error"])
	}
}

func TestUpload_MissingFile(t *testing.T) {
	h := &AvatarHandler{}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/avatars", nil)
	req.Header.Set("X-User-ID", "user1")
	w := httptest.NewRecorder()

	h.Upload(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestDeleteAvatar_MissingUserID(t *testing.T) {
	h := &AvatarHandler{}
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/avatars/some-id", nil)
	w := httptest.NewRecorder()

	h.DeleteAvatar(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestDeleteUserAvatar_MissingUserID(t *testing.T) {
	h := &AvatarHandler{}
	req := httptest.NewRequest(http.MethodDelete, "/api/v1/users/user1/avatar", nil)
	w := httptest.NewRecorder()

	h.DeleteUserAvatar(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
