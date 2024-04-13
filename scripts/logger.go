package main

import (
	"github.com/fatih/color"
)

var (
	infoLogger  = color.New(color.FgGreen).PrintfFunc()
	warnLogger  = color.New(color.FgYellow).PrintfFunc()
	errorLogger = color.New(color.FgRed).PrintfFunc()
)

func logInfo(format string, args ...interface{}) {
	infoLogger("INFO: "+format+"\n", args...)
}

func logWarning(format string, args ...interface{}) {
	warnLogger("WARNING: "+format+"\n", args...)
}

func logError(format string, args ...interface{}) {
	errorLogger("ERROR: "+format+"\n", args...)
}
