package service

import "testing"

func TestIsAllowedMimeType(t *testing.T) {
	allowed := []string{"image/jpeg", "image/png", "image/webp"}
	for _, mime := range allowed {
		if !IsAllowedMimeType(mime) {
			t.Errorf("expected %s to be allowed", mime)
		}
	}

	disallowed := []string{"image/gif", "image/bmp", "application/pdf", "text/plain"}
	for _, mime := range disallowed {
		if IsAllowedMimeType(mime) {
			t.Errorf("expected %s to be disallowed", mime)
		}
	}
}

func TestMaxUploadSize(t *testing.T) {
	expected := int64(10 << 20) // 10 МБ
	if MaxUploadSize != expected {
		t.Errorf("expected %d, got %d", expected, MaxUploadSize)
	}
}
