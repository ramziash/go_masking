package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"
)

// LogLevel represents different logging levels
type LogLevel int

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	FATAL
)

func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case FATAL:
		return "FATAL"
	default:
		return "UNKNOWN"
	}
}

// LogEntry represents a structured log entry
type LogEntry struct {
	Timestamp    time.Time              `json:"timestamp"`
	Level        string                 `json:"level"`
	Message      string                 `json:"message"`
	Component    string                 `json:"component,omitempty"`
	Function     string                 `json:"function,omitempty"`
	File         string                 `json:"file,omitempty"`
	Line         int                    `json:"line,omitempty"`
	Duration     *time.Duration         `json:"duration,omitempty"`
	RowCount     *int                   `json:"row_count,omitempty"`
	BatchCount   *int                   `json:"batch_count,omitempty"`
	ErrorDetails map[string]interface{} `json:"error_details,omitempty"`
	Metadata     map[string]interface{} `json:"metadata,omitempty"`
}

// CustomLogger provides enhanced logging capabilities
type CustomLogger struct {
	mu         sync.RWMutex
	level      LogLevel
	file       *os.File
	logger     *log.Logger
	jsonOutput bool
	component  string
	startTime  time.Time
	metrics    *LogMetrics
}

// LogMetrics tracks performance metrics
type LogMetrics struct {
	mu             sync.RWMutex
	TotalRows      int64
	TotalBatches   int64
	ErrorCount     int64
	WarningCount   int64
	StartTime      time.Time
	LastLogTime    time.Time
	ProcessingRate float64
}

// LoggerConfig holds logger configuration
type LoggerConfig struct {
	Level      LogLevel
	FilePath   string
	JSONOutput bool
	Component  string
	ToConsole  bool
	ToFile     bool
}

// NewCustomLogger creates a new enhanced logger
func NewCustomLogger(config LoggerConfig) (*CustomLogger, error) {
	var writers []io.Writer
	var file *os.File
	var err error

	// Setup file output if requested
	if config.ToFile && config.FilePath != "" {
		file, err = os.OpenFile(config.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open log file: %w", err)
		}
		writers = append(writers, file)
	}

	// Setup console output if requested
	if config.ToConsole {
		writers = append(writers, os.Stdout)
	}

	if len(writers) == 0 {
		writers = append(writers, os.Stdout) // Default to stdout
	}

	var writer io.Writer
	if len(writers) == 1 {
		writer = writers[0]
	} else {
		writer = io.MultiWriter(writers...)
	}

	logger := log.New(writer, "", 0) // We'll handle formatting ourselves

	return &CustomLogger{
		level:      config.Level,
		file:       file,
		logger:     logger,
		jsonOutput: config.JSONOutput,
		component:  config.Component,
		startTime:  time.Now(),
		metrics: &LogMetrics{
			StartTime:   time.Now(),
			LastLogTime: time.Now(),
		},
	}, nil
}

// Close closes the logger and its file handle
func (cl *CustomLogger) Close() error {
	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.file != nil {
		return cl.file.Close()
	}
	return nil
}

// SetLevel sets the minimum logging level
func (cl *CustomLogger) SetLevel(level LogLevel) {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.level = level
}

// shouldLog checks if a message should be logged based on level
func (cl *CustomLogger) shouldLog(level LogLevel) bool {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return level >= cl.level
}

// log is the core logging function
func (cl *CustomLogger) log(level LogLevel, message string, fields map[string]interface{}) {
	if !cl.shouldLog(level) {
		return
	}

	now := time.Now()

	// Update metrics
	cl.updateMetrics(level, now)

	entry := LogEntry{
		Timestamp: now,
		Level:     level.String(),
		Message:   message,
		Component: cl.component,
		Metadata:  fields,
	}

	// Add caller information for errors and warnings
	if level >= WARN {
		if pc, file, line, ok := runtime.Caller(2); ok {
			entry.File = file
			entry.Line = line
			if fn := runtime.FuncForPC(pc); fn != nil {
				entry.Function = fn.Name()
			}
		}
	}

	cl.mu.Lock()
	defer cl.mu.Unlock()

	if cl.jsonOutput {
		cl.logJSON(entry)
	} else {
		cl.logText(entry)
	}
}

// logJSON outputs structured JSON logs
func (cl *CustomLogger) logJSON(entry LogEntry) {
	if data, err := json.Marshal(entry); err == nil {
		cl.logger.Println(string(data))
	}
}

// logText outputs human-readable text logs
func (cl *CustomLogger) logText(entry LogEntry) {
	timestamp := entry.Timestamp.Format("2006-01-02 15:04:05.000")

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] %s", timestamp, entry.Level))

	if entry.Component != "" {
		sb.WriteString(fmt.Sprintf(" [%s]", entry.Component))
	}

	sb.WriteString(fmt.Sprintf(" %s", entry.Message))

	// Add duration if present
	if entry.Duration != nil {
		sb.WriteString(fmt.Sprintf(" (duration: %v)", *entry.Duration))
	}

	// Add row/batch counts if present
	if entry.RowCount != nil {
		sb.WriteString(fmt.Sprintf(" (rows: %d)", *entry.RowCount))
	}
	if entry.BatchCount != nil {
		sb.WriteString(fmt.Sprintf(" (batches: %d)", *entry.BatchCount))
	}

	// Add metadata
	if len(entry.Metadata) > 0 {
		sb.WriteString(" [")
		first := true
		for k, v := range entry.Metadata {
			if !first {
				sb.WriteString(", ")
			}
			sb.WriteString(fmt.Sprintf("%s: %v", k, v))
			first = false
		}
		sb.WriteString("]")
	}

	// Add error details if present
	if entry.File != "" && entry.Line > 0 {
		sb.WriteString(fmt.Sprintf(" (%s:%d)", entry.File, entry.Line))
	}

	cl.logger.Println(sb.String())
}

// updateMetrics updates internal performance metrics
func (cl *CustomLogger) updateMetrics(level LogLevel, now time.Time) {
	cl.metrics.mu.Lock()
	defer cl.metrics.mu.Unlock()

	cl.metrics.LastLogTime = now

	switch level {
	case ERROR, FATAL:
		cl.metrics.ErrorCount++
	case WARN:
		cl.metrics.WarningCount++
	}
}

// Public logging methods
func (cl *CustomLogger) Debug(message string, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	cl.log(DEBUG, message, f)
}

func (cl *CustomLogger) Info(message string, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	cl.log(INFO, message, f)
}

func (cl *CustomLogger) Warn(message string, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	cl.log(WARN, message, f)
}

func (cl *CustomLogger) Error(message string, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	cl.log(ERROR, message, f)
}

func (cl *CustomLogger) Fatal(message string, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	}
	cl.log(FATAL, message, f)
	os.Exit(1)
}

// Specialized logging methods for your use case
func (cl *CustomLogger) LogProgress(rowCount, batchCount int, message string) {
	cl.metrics.mu.Lock()
	cl.metrics.TotalRows = int64(rowCount)
	cl.metrics.TotalBatches = int64(batchCount)

	// Calculate processing rate
	elapsed := time.Since(cl.metrics.StartTime).Seconds()
	if elapsed > 0 {
		cl.metrics.ProcessingRate = float64(rowCount) / elapsed
	}
	cl.metrics.mu.Unlock()

	fields := map[string]interface{}{
		"rows_processed":    rowCount,
		"batches_processed": batchCount,
		"processing_rate":   fmt.Sprintf("%.0f rows/sec", cl.metrics.ProcessingRate),
		"elapsed_time":      time.Since(cl.metrics.StartTime).String(),
	}

	cl.log(INFO, message, fields)
}

func (cl *CustomLogger) LogTiming(operation string, duration time.Duration, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	} else {
		f = make(map[string]interface{})
	}

	f["operation"] = operation
	f["duration"] = duration.String()
	f["duration_ms"] = duration.Milliseconds()

	cl.log(INFO, fmt.Sprintf("Operation '%s' completed", operation), f)
}

func (cl *CustomLogger) LogError(operation string, err error, fields ...map[string]interface{}) {
	var f map[string]interface{}
	if len(fields) > 0 {
		f = fields[0]
	} else {
		f = make(map[string]interface{})
	}

	f["operation"] = operation
	f["error"] = err.Error()
	f["error_type"] = fmt.Sprintf("%T", err)

	cl.log(ERROR, fmt.Sprintf("Error in operation '%s': %v", operation, err), f)
}

// GetMetrics returns current performance metrics
func (cl *CustomLogger) GetMetrics() LogMetrics {
	cl.metrics.mu.RLock()
	defer cl.metrics.mu.RUnlock()

	// Return a copy without the mutex
	return LogMetrics{
		TotalRows:      cl.metrics.TotalRows,
		TotalBatches:   cl.metrics.TotalBatches,
		ErrorCount:     cl.metrics.ErrorCount,
		WarningCount:   cl.metrics.WarningCount,
		StartTime:      cl.metrics.StartTime,
		LastLogTime:    cl.metrics.LastLogTime,
		ProcessingRate: cl.metrics.ProcessingRate,
	}
}

// LogFinalSummary logs a final summary of the processing
func (cl *CustomLogger) LogFinalSummary() {
	metrics := cl.GetMetrics()
	totalDuration := time.Since(cl.startTime)

	fields := map[string]interface{}{
		"total_rows":          metrics.TotalRows,
		"total_batches":       metrics.TotalBatches,
		"total_duration":      totalDuration.String(),
		"avg_processing_rate": fmt.Sprintf("%.0f rows/sec", metrics.ProcessingRate),
		"error_count":         metrics.ErrorCount,
		"warning_count":       metrics.WarningCount,
	}

	cl.log(INFO, "Processing completed successfully", fields)
}
