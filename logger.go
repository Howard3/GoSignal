package gosignal

import "fmt"

type Logger interface {
	Error(string)
}

func logError(l Logger, err error) {
	if l != nil {
		l.Error(err.Error())
	}
}

func logIfErrWithMsg(l Logger, err error, msg string) {
	if err != nil {
		logError(l, fmt.Errorf("%s: %w", msg, err))
	}
}
