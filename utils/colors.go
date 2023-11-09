package utils

import "github.com/fatih/color"

var (
	ERROR_PRINT = color.New(color.FgRed).PrintfFunc()
	WARN_PRINT  = color.New(color.FgYellow).PrintfFunc()
	INFO_PRINT  = color.New(color.FgBlue).PrintfFunc()
	SUCCESS_PRINT = color.New(color.FgGreen).PrintfFunc()
)
