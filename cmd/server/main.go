package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/anon-d/gophProfile/internal/app"
)

func main() {

	app, err := app.NewApp()
	if err != nil {
		os.Exit(2)
	}

	go func() {
		log.Println("server started")
		if err := app.Run(); err != nil {
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down...")
	if err := app.Shutdown(); err != nil {
		log.Fatal(err)
	}
	log.Println("server stopped")
}
