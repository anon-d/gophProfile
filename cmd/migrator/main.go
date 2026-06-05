package main

import (
	"log"
	"os"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://gophprofile:gophprofile@localhost:5432/gophprofile?sslmode=disable"
	}

	m, err := migrate.New("file://migrations", dsn)
	if err != nil {
		log.Fatalf("migrate new: %v", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("migrate up: %v", err)
	}

	log.Println("migrations applied successfully")
}
