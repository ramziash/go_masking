package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func WriteToCSV(filePath string, chunkChan chan [][]string) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	var rowCount int
	for chunk := range chunkChan {
		for _, row := range chunk {
			// fmt.Printf("writting row number %d of size: %d ", i, len(row))
			if err := writer.Write(row); err != nil {
				return err
			}
			rowCount++
		}
		fmt.Printf("Processed %d rows so far \n", rowCount)
		customLogger.Printf("Processed %d rows so far", rowCount)
	}
	customLogger.Printf("Processed %d rows final", rowCount)

	return nil
}
