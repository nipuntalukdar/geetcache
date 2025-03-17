package main

import (
	"log"
	"os"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/nipuntalukdar/rollingwriter"
)

type MyLogger struct {
	hclog.Logger
}

var (
	SLOG        MyLogger
	SLOG_WRITER rollingwriter.RollingWriter
)

func (mylogger MyLogger) Fatal(msg string, args ...interface{}) {
	mylogger.Error("FATAL "+msg, args)
	log.Println("Fatal ", SLOG_WRITER)
	SLOG_WRITER.Close()
	os.Exit(1)
}

func init_logger(logfileconfig string) {
	var err error
	SLOG_WRITER, err = rollingwriter.NewWriterFromConfigFile(logfileconfig)
	log.Print("Hello", SLOG_WRITER)
	if err != nil {
		panic(err)
	}
	logger := hclog.New(&hclog.LoggerOptions{Name: "RaftDemo", Output: SLOG_WRITER,
		Level: hclog.Debug})
	SLOG = MyLogger{Logger: logger}

}
