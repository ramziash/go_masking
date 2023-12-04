package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"
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

func createKeyValuePairs(m map[string]string) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s=\"%s\"\n", key, value)
	}
	return b.String()
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
	chunkChan := make(chan [][]string, chunkSize)
	processedChunkChan := make(chan [][]string, chunkSize)

	go func() {
		if err := ReadParquetInChunks(*inputPath, chunkChan, chunkSize); err != nil {
			panic(err)
		}
	}()

	var wg sync.WaitGroup

	// Start processing each chunk concurrently as they are read
	go func() {
		for chunk := range chunkChan {
			wg.Add(1)
			go func(data [][]string) {
				defer wg.Done()
				columnsToMask := []int{0} // Specify columns to mask by index

				maskedChunk := MaskDataParallel(data, columnsToMask)

				combinedChunk := make([][]string, len(data))
				for i, row := range data {
					combinedChunk[i] = append(row, maskedChunk[i]...)
				}
				processedChunkChan <- combinedChunk
			}(chunk)
		}

		wg.Wait()
		close(processedChunkChan)
	}()

	// Write processed data to CSV as chunks are processed
	filePath := "output.csv"
	if err := WriteToCSV(filePath, processedChunkChan); err != nil {
		panic(err)
	}
}
