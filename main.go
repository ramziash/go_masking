package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var logger *CustomLogger

func timer(name string, quiet bool) func() {
	start := time.Now()
	return func() {
		duration := time.Since(start)
		logger.LogTiming(name, duration)
		if !quiet {
			fmt.Printf("%s took %v\n", name, duration)
		}
	}
}

func initImprovedLogger(quiet, verbose, jsonLogs bool) error {
	// Determine log level
	logLevel := INFO
	if verbose {
		logLevel = DEBUG
	}

	config := LoggerConfig{
		Level:      logLevel,
		FilePath:   "app.log",
		JSONOutput: jsonLogs,
		Component:  "ParquetMasker",
		ToConsole:  !quiet, // Disable console output in quiet mode
		ToFile:     true,   // Always log to file
	}

	var err error
	logger, err = NewCustomLogger(config)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}

	if !quiet {
		logger.Info("Logger initialized successfully")
	}
	return nil
}

// parseColumns parses a comma-separated string of column indexes
func parseColumns(columnStr string) ([]int, error) {
	if columnStr == "" {
		return []int{3}, nil // Default to column 3
	}

	parts := strings.Split(columnStr, ",")
	columns := make([]int, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		col, err := strconv.Atoi(part)
		if err != nil {
			return nil, fmt.Errorf("invalid column index '%s': %w", part, err)
		}

		if col < 0 {
			return nil, fmt.Errorf("column index must be non-negative, got %d", col)
		}

		columns = append(columns, col)
	}

	if len(columns) == 0 {
		return []int{3}, nil // Default to column 3
	}

	return columns, nil
}

type AppConfig struct {
	InputPath     string
	OutputFile    string
	ColumnsToMask []int
	ChunkSize     int
	Quiet         bool
	Verbose       bool
	JsonLogs      bool
}

func parseCommandLineArgs() (*AppConfig, error) {
	inputPath := flag.String("input_path", "creditagreementliabledebtor.snappy.parquet", "insert path to the file to mask")
	quiet := flag.Bool("quiet", false, "run in quiet mode (no console output)")
	verbose := flag.Bool("verbose", false, "run in verbose mode (debug level logging)")
	jsonLogs := flag.Bool("json", false, "output logs in JSON format")
	columnsStr := flag.String("columns", "3", "comma-separated list of column indexes to mask (e.g., '3' or '1,3,5')")
	flag.Parse()

	columnsToMask, err := parseColumns(*columnsStr)
	if err != nil {
		return nil, errors.New("error parsing columns: " + err.Error())
	}

	return &AppConfig{
		InputPath:     *inputPath,
		OutputFile:    "output.csv",
		ColumnsToMask: columnsToMask,
		ChunkSize:     10000,
		Quiet:         *quiet,
		Verbose:       *verbose,
		JsonLogs:      *jsonLogs,
	}, nil
}

func setupApplication(config *AppConfig) (*CSVWriter, error) {
	err := initImprovedLogger(config.Quiet, config.Verbose, config.JsonLogs)
	if err != nil {
		return nil, err
	}

	logger.Info("Starting parquet masking process", map[string]interface{}{
		"input_file":      config.InputPath,
		"chunk_size":      config.ChunkSize,
		"output_file":     config.OutputFile,
		"columns_to_mask": config.ColumnsToMask,
	})

	os.Remove(config.OutputFile)
	csvWriter, err := NewCSVWriter(config.OutputFile)
	if err != nil {
		logger.LogError("CSV writer creation", err)
		return nil, err
	}

	logger.Info("Initializing CSV output and reading schema")
	if err := readWriteParquetSchema(config.InputPath); err != nil {
		logger.LogError("Reading parquet schema", err)
		csvWriter.Close()
		return nil, err
	}

	return csvWriter, nil
}

func startParquetReader(inputPath string, chunkChan chan<- [][]string, chunkSize int) {
	go func() {
		if err := ReadParquetInChunks(inputPath, chunkChan, chunkSize); err != nil {
			logger.LogError("Reading parquet chunks", err, map[string]interface{}{
				"input_path": inputPath,
				"chunk_size": chunkSize,
			})
			panic(err)
		}
		logger.Debug("Finished reading all parquet chunks")
	}()
}

func startBatchProcessor(chunkChan <-chan [][]string, processedChunkChan chan<- [][]string, columnsToMask []int) {
	go func() {
		batchCount := 0
		for batch := range chunkChan {
			batchCount++

			logger.Debug("Processing batch", map[string]interface{}{
				"batch_number":    batchCount,
				"batch_size":      len(batch),
				"columns_to_mask": columnsToMask,
				"masking_strategy": func() string {
					if len(batch) > 500 {
						return "parallel_workers"
					}
					return "simple_parallel"
				}(),
			})

			var maskedBatch [][]string
			if len(batch) > 500 {
				maskedBatch = MaskBatchParallelWorkers(batch, columnsToMask)
			} else {
				maskedBatch = MaskBatchParallel(batch, columnsToMask)
			}

			processedChunkChan <- maskedBatch
		}
		close(processedChunkChan)
		logger.Debug("Finished processing all batches", map[string]interface{}{
			"total_batches": batchCount,
		})
	}()
}

func writeProcessedData(csvWriter *CSVWriter, processedChunkChan <-chan [][]string) (int, int, error) {
	logger.Info("Starting CSV writing process")
	var rowCount int
	var batchCount int
	const flushInterval = 5

	for batch := range processedChunkChan {
		if err := csvWriter.WriteRowsNoFlush(batch); err != nil {
			logger.LogError("Writing batch to CSV", err, map[string]interface{}{
				"batch_number": batchCount,
				"batch_size":   len(batch),
			})
			return rowCount, batchCount, err
		}

		batchCount++
		rowCount += len(batch)

		if batchCount%flushInterval == 0 {
			if err := csvWriter.Flush(); err != nil {
				logger.LogError("Flushing CSV writer", err, map[string]interface{}{
					"batch_number": batchCount,
				})
				return rowCount, batchCount, err
			}
		}

		if rowCount%10000 == 0 {
			logger.LogProgress(rowCount, batchCount, "Processing progress update")
		}
	}

	logger.Debug("Performing final flush")
	if err := csvWriter.Flush(); err != nil {
		logger.LogError("Final flush", err)
		return rowCount, batchCount, err
	}

	return rowCount, batchCount, nil
}

func main() {
	config, err := parseCommandLineArgs()
	if err != nil {
		log.Fatalf("Error parsing command line arguments: %v", err)
	}

	defer timer("main", config.Quiet)()
	defer func() {
		if logger != nil {
			logger.LogFinalSummary()
			logger.Close()
		}
	}()

	csvWriter, err := setupApplication(config)
	if err != nil {
		log.Fatal(err)
	}
	defer csvWriter.Close()

	chunkChan := make(chan [][]string, 10)
	processedChunkChan := make(chan [][]string, 10)

	startParquetReader(config.InputPath, chunkChan, config.ChunkSize)
	startBatchProcessor(chunkChan, processedChunkChan, config.ColumnsToMask)

	rowCount, batchCount, err := writeProcessedData(csvWriter, processedChunkChan)
	if err != nil {
		panic(err)
	}

	logger.Info("Processing completed successfully", map[string]interface{}{
		"total_rows_processed":    rowCount,
		"total_batches_processed": batchCount,
	})
}
