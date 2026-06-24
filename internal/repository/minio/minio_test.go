package minio

import "testing"

func TestAvatarKey(t *testing.T) {
	key := AvatarKey("user-1", "avatar-abc")
	expected := "avatars/user-1/avatar-abc"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestThumbnailKey(t *testing.T) {
	key := ThumbnailKey("avatar-abc", "100x100")
	expected := "thumbnails/avatar-abc/100x100.jpg"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}

func TestThumbnailKey_300(t *testing.T) {
	key := ThumbnailKey("avatar-xyz", "300x300")
	expected := "thumbnails/avatar-xyz/300x300.jpg"
	if key != expected {
		t.Errorf("expected %s, got %s", expected, key)
	}
}
