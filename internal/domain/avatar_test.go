package domain

import (
	"encoding/json"
	"testing"
)

func TestAvatarUploadEvent_JSON(t *testing.T) {
	event := AvatarUploadEvent{
		AvatarID: "abc-123",
		UserID:   "user-1",
		S3Key:    "avatars/user-1/abc-123",
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded AvatarUploadEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.AvatarID != event.AvatarID {
		t.Errorf("expected %s, got %s", event.AvatarID, decoded.AvatarID)
	}
	if decoded.UserID != event.UserID {
		t.Errorf("expected %s, got %s", event.UserID, decoded.UserID)
	}
	if decoded.S3Key != event.S3Key {
		t.Errorf("expected %s, got %s", event.S3Key, decoded.S3Key)
	}
}

func TestAvatarDeleteEvent_JSON(t *testing.T) {
	event := AvatarDeleteEvent{
		AvatarID: "abc-123",
		S3Keys:   []string{"avatars/user-1/abc-123", "thumbnails/abc-123/100x100.jpg"},
	}

	data, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded AvatarDeleteEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.AvatarID != event.AvatarID {
		t.Errorf("expected %s, got %s", event.AvatarID, decoded.AvatarID)
	}
	if len(decoded.S3Keys) != 2 {
		t.Errorf("expected 2 keys, got %d", len(decoded.S3Keys))
	}
}

func TestAvatarMetadata_JSON(t *testing.T) {
	meta := AvatarMetadata{
		ID:       "abc-123",
		UserID:   "user-1",
		FileName: "photo.jpg",
		MimeType: "image/jpeg",
		Size:     1024,
		Thumbnails: []Thumbnail{
			{Size: "100x100", URL: "/api/v1/avatars/abc-123?size=100x100"},
			{Size: "300x300", URL: "/api/v1/avatars/abc-123?size=300x300"},
		},
	}

	data, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded AvatarMetadata
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.FileName != "photo.jpg" {
		t.Errorf("expected photo.jpg, got %s", decoded.FileName)
	}
	if len(decoded.Thumbnails) != 2 {
		t.Errorf("expected 2 thumbnails, got %d", len(decoded.Thumbnails))
	}
}

func TestConstants(t *testing.T) {
	if UploadStatusUploading != "uploading" {
		t.Error("UploadStatusUploading mismatch")
	}
	if ProcessingStatusPending != "pending" {
		t.Error("ProcessingStatusPending mismatch")
	}
	if ProcessingStatusCompleted != "completed" {
		t.Error("ProcessingStatusCompleted mismatch")
	}
}
