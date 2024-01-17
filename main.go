package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"
)

func timer(name string) func() {
	start := time.Now()
	return func() {
		customLogger.Printf("%s took %v\n", name, time.Since(start))
		fmt.Printf("%s took %v\n", name, time.Since(start))
	}
}

var customLogger *log.Logger

func initlogger(logFilePath string) error {
	logFile, err := os.OpenFile(logFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	customLogger = log.New(logFile, "CUSTOM LOGGER: ", log.LstdFlags)

	return nil
}

func main() {
	// if there are errors (error at chunk 0:). make sure the file and the struct being read are the same.

	inputPath := flag.String("input_path", "creditagreementliabledebtor.snappy.parquet", "insert path to the file to mask")
	flag.Parse()

	defer timer("main")()

	err := initlogger("app.log")
	if err != nil {
		log.Fatal(err)
	}

	const chunkSize = 1000
	chunkChan := make(chan []string, chunkSize)
	processedChunkChan := make(chan []string, chunkSize)
	const outputFile = "output.csv"

	os.Remove(outputFile)
	csvWriter, err := NewCSVWriter(outputFile)
	if err != nil {
		panic(err)
	}
	defer csvWriter.Close()

	// writing the headers to a CSV file
	if err := readWriteParquetSchema(*inputPath); err != nil {
		panic(err)
	}

	go func() {
		if err := ReadParquetInChunks(*inputPath, chunkChan, chunkSize, *csvWriter); err != nil {
			panic(err)
		}
	}()

	// Start processing each chunk concurrently as they are read

	go func() {
		for chunk := range chunkChan {
			columnsToMask := []int{3}                           // Specify columns to mask by index
			firstItemData := columnsToMask[0]                   // used for multi-column
			lastItemData := columnsToMask[len(columnsToMask)-1] // used for multi-column

			maskedChunk := MaskDataParallel(chunk, columnsToMask)

			combinedChunk := make([]string, len(chunk))

			combinedChunk = append(combinedChunk, chunk[0:firstItemData]...) // stop at the first column
			combinedChunk = append(combinedChunk, maskedChunk...)
			combinedChunk = append(combinedChunk, chunk[lastItemData:]...)

			processedChunkChan <- combinedChunk
		}
		close(processedChunkChan)
	}()

	// Write processed data to CSV as chunks are processed
	var rowCount int
	for chunk := range processedChunkChan {
		if err := csvWriter.Write(chunk); err != nil {
			panic(err)
		}
		rowCount++
		// fmt.Printf("Processed %d rows so far \n", rowCount)
		// customLogger.Printf("Processed %d rows so far", rowCount)
	}
	customLogger.Printf("Processed %d rows final", rowCount)

}
