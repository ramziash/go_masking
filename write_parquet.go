package main

import (
	"encoding/csv"
	"os"
)

type CSVWriter struct {
	file   *os.File
	writer *csv.Writer
}

func NewCSVWriter(writePath string) (*CSVWriter, error) {
	writeFile, err := os.OpenFile(writePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(writeFile)

	return &CSVWriter{
		file:   writeFile,
		writer: writer,
	}, nil

}

func (cw *CSVWriter) Write(csvRow []string) error {
	if err := cw.writer.Write(csvRow); err != nil {
		panic(err)
	}
	cw.writer.Flush()
	return cw.writer.Error()
}

func (cw *CSVWriter) Close() error {
	cw.writer.Flush()

	if err := cw.writer.Error(); err != nil {
		cw.file.Close()
		return err
	}
	return cw.file.Close()
}

func (cw *CSVWriter) DeleteOutputFile() error {
	err := cw.Close()
	if err != nil {
		return err
	}

	return os.Remove(cw.file.Name())

}

// func WriteToCSV(filePath string, chunkChan chan [][]string) error {
// 	file, err := os.Create(filePath)
// 	if err != nil {
// 		return err
// 	}
// 	defer file.Close()

// 	writer := csv.NewWriter(file)
// 	defer writer.Flush()

// 	var rowCount int
// 	for chunk := range chunkChan {
// 		for _, row := range chunk {
// 			// fmt.Printf("writting row number %d of size: %d ", i, len(row))
// 			if err := writer.Write(row); err != nil {
// 				return err
// 			}
// 			rowCount++
// 		}
// 		fmt.Printf("Processed %d rows so far \n", rowCount)
// 		customLogger.Printf("Processed %d rows so far", rowCount)
// 	}
// 	customLogger.Printf("Processed %d rows final", rowCount)

// 	return nil
// }
