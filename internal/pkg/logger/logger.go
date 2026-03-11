package logger

import (
	"net/http"
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// Log singleton value
var Log *zap.Logger
var once sync.Once

func GetLogger() {
	once.Do(func() {
		stdout := zapcore.AddSync(os.Stdout)

		level := zap.NewAtomicLevelAt(zap.InfoLevel)
		file := zapcore.AddSync(&lumberjack.Logger{
			Filename:   "../../logs/app.log",
			MaxSize:    10, // megabytes
			MaxBackups: 3,
			MaxAge:     7, // days
			Compress:   true,
		})

		productionCfg := zap.NewProductionEncoderConfig()
		productionCfg.TimeKey = "timestamp"
		productionCfg.EncodeTime = zapcore.TimeEncoderOfLayout("2006-01-02 15:04:05")
		fileEncoder := zapcore.NewJSONEncoder(productionCfg)

		developmentCfg := zap.NewDevelopmentEncoderConfig()
		developmentCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
		developmentCfg.EncodeTime = zapcore.TimeEncoderOfLayout("15:04:05")
		consoleEncoder := zapcore.NewConsoleEncoder(developmentCfg)

		core := zapcore.NewTee(
			zapcore.NewCore(consoleEncoder, stdout, level),
			zapcore.NewCore(fileEncoder, file, level))

		Log = zap.New(core)
	})
}

type (
	responseData struct {
		status int
		size   int
	}
	loggingResponseWriter struct {
		http.ResponseWriter
		responseData *responseData
	}
)

func (r *loggingResponseWriter) Write(b []byte) (int, error) {
	size, err := r.ResponseWriter.Write(b)
	r.responseData.size += size
	return size, err
}

func (r *loggingResponseWriter) WriteHeader(statusCode int) {
	r.ResponseWriter.WriteHeader(statusCode)
	r.responseData.status = statusCode
}
