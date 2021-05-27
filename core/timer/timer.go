package timer

import (
	"math"
	"sync"
	"time"
)

// Timer is the timer manager, which uses ticks to calculate the timing interval.
type Timer struct {
	mu      sync.RWMutex
	queue   *priorityQueue // queue is a priority queue based on heap structure.
	status  int64          // status is the current timer status.
	ticks   int64          // ticks is the proceeded interval number by the timer.
	options TimerOptions   // timer options is used for timer configuration.
}

// TimerOptions is the configuration object for Timer.
type TimerOptions struct {
	Interval time.Duration // Interval is the interval escaped of the timer.
}

const (
	StatusReady          = 0             // Job or Timer is ready for running.
	StatusRunning        = 1             // Job or Timer is already running.
	StatusStopped        = 2             // Job or Timer is stopped.
	StatusClosed         = -1            // Job or Timer is closed and waiting to be deleted.
	panicExit            = "exit"        // panicExit is used for custom job exit with panic.
	defaultTimes         = math.MaxInt32 // defaultTimes is the default limit running times, a big number.
)

var (
	defaultTimer    = New()
	defaultInterval = 100 * time.Millisecond
)

// DefaultOptions creates and returns a default options object for Timer creation.
func DefaultOptions() TimerOptions {
	return TimerOptions{
		Interval: defaultInterval,
	}
}

// SetTimeout runs the job once after duration of <delay>.
// It is like the one in javascript.
func SetTimeout(delay time.Duration, job JobFunc) {
	AddOnce(delay, job)
}

// SetInterval runs the job every duration of <delay>.
// It is like the one in javascript.
func SetInterval(interval time.Duration, job JobFunc) {
	Add(interval, job)
}

// Add adds a timing job to the default timer, which runs in interval of <interval>.
func Add(interval time.Duration, job JobFunc) *Job {
	return defaultTimer.Add(interval, job)
}

// AddJob adds a timing job to the default timer with detailed parameters.
//
// The parameter <interval> specifies the running interval of the job.
//
// The parameter <singleton> specifies whether the job running in singleton mode.
// There's only one of the same job is allowed running when its a singleton mode job.
//
// The parameter <times> specifies limit for the job running times, which means the job
// exits if its run times exceeds the <times>.
//
// The parameter <status> specifies the job status when it's firstly added to the timer.
func AddJob(interval time.Duration, job JobFunc, singleton bool, times int, status int) *Job {
	return defaultTimer.AddJob(interval, job, singleton, times, status)
}

// AddSingleton is a convenience function for add singleton mode job.
func AddSingleton(interval time.Duration, job JobFunc) *Job {
	return defaultTimer.AddSingleton(interval, job)
}

// AddOnce is a convenience function for adding a job which only runs once and then exits.
func AddOnce(interval time.Duration, job JobFunc) *Job {
	return defaultTimer.AddOnce(interval, job)
}

// AddTimes is a convenience function for adding a job which is limited running times.
func AddTimes(interval time.Duration, times int, job JobFunc) *Job {
	return defaultTimer.AddTimes(interval, times, job)
}

// DelayAdd adds a timing job after delay of <interval> duration.
// Also see Add.
func DelayAdd(delay time.Duration, interval time.Duration, job JobFunc) {
	defaultTimer.DelayAdd(delay, interval, job)
}

// DelayAddJob adds a timing job after delay of <interval> duration.
// Also see AddJob.
func DelayAddJob(delay time.Duration, interval time.Duration, job JobFunc, singleton bool, times int, status int) {
	defaultTimer.DelayAddJob(delay, interval, job, singleton, times, status)
}

// DelayAddSingleton adds a timing job after delay of <interval> duration.
// Also see AddSingleton.
func DelayAddSingleton(delay time.Duration, interval time.Duration, job JobFunc) {
	defaultTimer.DelayAddSingleton(delay, interval, job)
}

// DelayAddOnce adds a timing job after delay of <interval> duration.
// Also see AddOnce.
func DelayAddOnce(delay time.Duration, interval time.Duration, job JobFunc) {
	defaultTimer.DelayAddOnce(delay, interval, job)
}

// DelayAddTimes adds a timing job after delay of <interval> duration.
// Also see AddTimes.
func DelayAddTimes(delay time.Duration, interval time.Duration, times int, job JobFunc) {
	defaultTimer.DelayAddTimes(delay, interval, times, job)
}

// Exit is used in timing job internally, which exits and marks it closed from timer.
// The timing job will be automatically removed from timer later. It uses "panic-recover"
// mechanism internally implementing this feature, which is designed for simplification
// and convenience.
func Exit() {
	panic(panicExit)
}
