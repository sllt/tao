package console

import (
	"fmt"
	"log"

	"github.com/logrusorgru/aurora"
)

// Console describes an abstract printer.
type Console interface {
	Info(msg ...interface{})
	InfoF(format string, msg ...interface{})
	Debug(msg ...interface{})
	DebugF(format string, msg ...interface{})
	Warning(msg ...interface{})
	WarningF(format string, msg ...interface{})
	Error(msg ...interface{})
	ErrorF(format string, msg ...interface{})
	Fatal(msg ...interface{})
	FatalF(format string, msg ...interface{})
	Panic(msg ...interface{})
	PanicF(format string, msg ...interface{})
}

// Assert *colorConsole implements Console.
var _ Console = (*colorConsole)(nil)

// NewColorConsole returns an instance of Console.
func NewColorConsole() Console {
	return &colorConsole{}
}

type colorConsole struct{}

func (c *colorConsole) Info(msg ...interface{}) {
	fmt.Println(aurora.Green(fmt.Sprint(msg...)))
}

func (c *colorConsole) InfoF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(aurora.Green(msg))
}

func (c *colorConsole) Debug(msg ...interface{}) {
	fmt.Println(aurora.Blue(fmt.Sprint(msg...)))
}

func (c *colorConsole) DebugF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(aurora.Blue(msg))
}

func (c *colorConsole) Warning(msg ...interface{}) {
	fmt.Println(aurora.Yellow(fmt.Sprint(msg...)))
}

func (c *colorConsole) WarningF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(aurora.Yellow(msg))
}

func (c *colorConsole) Error(msg ...interface{}) {
	fmt.Println(aurora.Red(fmt.Sprint(msg...)))
}

func (c *colorConsole) ErrorF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	fmt.Println(aurora.Red(msg))
}

func (c *colorConsole) Fatal(msg ...interface{}) {
	log.Fatalln(fmt.Sprint(msg...))
}

func (c *colorConsole) FatalF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	log.Fatalln(aurora.Red(msg))
}

func (c *colorConsole) Panic(msg ...interface{}) {
	panic(fmt.Sprint(msg...))
}

func (c *colorConsole) PanicF(format string, a ...interface{}) {
	msg := fmt.Sprintf(format, a...)
	panic(msg)
}
