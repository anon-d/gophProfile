package logger

import (
	"log/slog"
	"testing"
)

func TestSetup(t *testing.T) {
	tests := []struct {
		level    string
		expected slog.Level
	}{
		{"debug", slog.LevelDebug},
		{"info", slog.LevelInfo},
		{"warn", slog.LevelWarn},
		{"warning", slog.LevelWarn},
		{"error", slog.LevelError},
		{"unknown", slog.LevelInfo},
		{"", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			l := Setup(tt.level)
			if l == nil {
				t.Fatal("logger is nil")
			}
			if !l.Handler().Enabled(nil, tt.expected) {
				t.Errorf("expected level %v to be enabled", tt.expected)
			}
		})
	}
}
