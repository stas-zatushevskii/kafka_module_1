package main

import (
	"kafka_module_1/internal/app"
	"kafka_module_1/internal/pkg/logger"

	"go.uber.org/zap"
)

func main() {

	// initialize logger
	logger.GetLogger()

	application, err := app.New()
	if err != nil {
		logger.Log.Fatal("Failed to create Application", zap.Error(err))
		return
	}

	err = application.Start()
	if err != nil {
		logger.Log.Fatal("Failed to start Application", zap.Error(err))
		return
	}
}
