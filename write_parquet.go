package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

const (
	flushErrorMsg        = "failed to flush CSV writer: %w"
	closedWriterErrorMsg = "cannot write to closed CSV writer"
)

type CSVWriter struct {
	file     *os.File
	writer   *csv.Writer
	filePath string
	closed   bool
}

func NewCSVWriter(writePath string) (*CSVWriter, error) {
	writeFile, err := os.OpenFile(writePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", writePath, err)
	}

	writer := csv.NewWriter(writeFile)

	return &CSVWriter{
		file:     writeFile,
		writer:   writer,
		filePath: writePath,
		closed:   false,
	}, nil
}

// Write writes a single row to the CSV file
func (cw *CSVWriter) Write(csvRow []string) error {
	if cw.closed {
		return errors.New(closedWriterErrorMsg)
	}

	if csvRow == nil {
		return fmt.Errorf("cannot write nil row")
	}

	if err := cw.writer.Write(csvRow); err != nil {
		return fmt.Errorf("failed to write CSV row: %w", err)
	}

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return fmt.Errorf(flushErrorMsg, err)
	}

	return nil
}

// WriteRows writes multiple rows efficiently in a single operation
func (cw *CSVWriter) WriteRows(rows [][]string) error {
	if cw.closed {
		return errors.New(closedWriterErrorMsg)
	}

	if rows == nil {
		return fmt.Errorf("cannot write nil rows")
	}

	for i, row := range rows {
		if err := cw.writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row %d: %w", i, err)
		}
	}

	// Only flush after writing all rows in the batch
	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return fmt.Errorf(flushErrorMsg, err)
	}

	return nil
}

// WriteRowsNoFlush writes multiple rows without flushing (for high performance)
func (cw *CSVWriter) WriteRowsNoFlush(rows [][]string) error {
	if cw.closed {
		return errors.New(closedWriterErrorMsg)
	}

	if rows == nil {
		return fmt.Errorf("cannot write nil rows")
	}

	for i, row := range rows {
		if err := cw.writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row %d: %w", i, err)
		}
	}

	return nil
}

// Flush manually flushes the writer without closing
func (cw *CSVWriter) Flush() error {
	if cw.closed {
		return fmt.Errorf("cannot flush closed CSV writer")
	}

	cw.writer.Flush()
	if err := cw.writer.Error(); err != nil {
		return fmt.Errorf(flushErrorMsg, err)
	}

	return nil
}

func (cw *CSVWriter) Close() error {
	if cw.closed {
		return nil // Already closed
	}

	cw.closed = true
	cw.writer.Flush()

	var flushErr error
	if err := cw.writer.Error(); err != nil {
		flushErr = fmt.Errorf("writer flush error: %w", err)
	}

	// Always attempt to close the file, even if flush failed
	if err := cw.file.Close(); err != nil {
		if flushErr != nil {
			return fmt.Errorf("multiple errors - flush: %v, close: %w", flushErr, err)
		}
		return fmt.Errorf("failed to close file: %w", err)
	}

	return flushErr
}

func (cw *CSVWriter) DeleteOutputFile() error {
	err := cw.Close()
	if err != nil {
		return err
	}

	return os.Remove(cw.filePath)
}
