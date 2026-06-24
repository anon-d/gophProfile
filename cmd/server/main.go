package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/anon-d/gophProfile/internal/app"
)

func main() {
	// Контекст отменяется при SIGINT/SIGTERM.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	application, err := app.NewApp()
	if err != nil {
		log.Fatalf("init app: %v", err)
	}

	// Запускаем сервер в горутине.
	errCh := make(chan error, 1)
	go func() {
		if err := application.Run(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()
	log.Println("server started")

	// Ждём сигнал или ошибку сервера.
	select {
	case <-ctx.Done():
		log.Println("received shutdown signal")
	case err := <-errCh:
		if err != nil {
			log.Printf("server error: %v", err)
		}
	}

	log.Println("shutting down...")
	if err := application.Shutdown(); err != nil {
		log.Fatalf("shutdown: %v", err)
	}
	log.Println("server stopped")
}
