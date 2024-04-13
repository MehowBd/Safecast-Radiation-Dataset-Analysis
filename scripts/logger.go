package main

import (
    "github.com/fatih/color"
)

var (
    infoLog  = color.New(color.FgHiBlue).PrintfFunc() // Change info to blue
    warnLog  = color.New(color.FgHiYellow).PrintfFunc()
    errorLog = color.New(color.FgHiRed).PrintfFunc()
    successLog = color.New(color.FgHiGreen).PrintfFunc() // Add success log in green
)

func logInfo(format string, args ...interface{}) {
    infoLog("INFO: "+format+"\n", args...)
}

func logWarning(format string, args ...interface{}) {
    warnLog("WARNING: "+format+"\n", args...)
}

func logError(format string, args ...interface{}) {
    errorLog("ERROR: "+format+"\n", args...)
}

func logSuccess(format string, args ...interface{}) {
    successLog("SUCCESS: "+format+"\n", args...)
}
