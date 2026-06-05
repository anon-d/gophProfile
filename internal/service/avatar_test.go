package service

import "testing"

func TestAllowedMimeTypes(t *testing.T) {
	allowed := []string{"image/jpeg", "image/png", "image/webp"}
	for _, mime := range allowed {
		if !AllowedMimeTypes[mime] {
			t.Errorf("expected %s to be allowed", mime)
		}
	}

	disallowed := []string{"image/gif", "image/bmp", "application/pdf", "text/plain"}
	for _, mime := range disallowed {
		if AllowedMimeTypes[mime] {
			t.Errorf("expected %s to be disallowed", mime)
		}
	}
}

func TestMaxFileSize(t *testing.T) {
	expected := int64(10 << 20) // 10 МБ
	if MaxFileSize != expected {
		t.Errorf("expected %d, got %d", expected, MaxFileSize)
	}
}
