package main

import (
	"errors"
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	dsn, ok := os.LookupEnv("DATABASE_URL")
	if !ok || dsn == "" {
		log.Fatal("DATABASE_URL environment variable is required")
	}

	m, err := migrate.New("file://migrations", dsn)
	if err != nil {
		log.Fatalf("migrate new: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		log.Fatalf("migrate up: %v", err)
	}

	log.Println("migrations applied successfully")
}
