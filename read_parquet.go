package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"reflect"
	"strconv"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func readWriteParquetSchema(filePath string) error {

	// reading the first row to get the colums
	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		return err
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		return err
	}
	defer pr.ReadStop()

	schemaElements := pr.SchemaHandler.ValueColumns
	// fmt.Println(schemaElements)

	// writing the schema to the file
	var columnNamesCsv []string
	delimeter := []byte{0x01}

	for _, columnName := range schemaElements {

		columnNameSplit := bytes.Split([]byte(columnName), delimeter)

		cleanedColName := string(columnNameSplit[len(columnNameSplit)-1])
		columnNamesCsv = append(columnNamesCsv, cleanedColName)
	}

	writeFileName := "output.csv"
	writeFile, err := os.Create(writeFileName)
	if err != nil {
		return err
	}
	defer writeFile.Close()

	writer := csv.NewWriter(writeFile)
	defer writer.Flush()

	if err := writer.Write(columnNamesCsv); err != nil {
		panic(err)
	}

	return nil
}

func ReadParquetInChunks(filePath string, chunkChan chan<- []string, chunkSize int, csvWriter CSVWriter) error {

	fr, err := local.NewLocalFileReader(filePath)
	if err != nil {
		panic(err)
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, nil, 4)
	if err != nil {
		panic(err)
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())

	for i := 0; i < num; i += chunkSize {

		if i+chunkSize > num {
			i = num - chunkSize
		}

		data, err := pr.ReadByNumber(chunkSize)
		if err != nil {
			panic(err)
		}

		for _, i := range data {
			csvRow, err := SchemaLossless(i)
			if err != nil {
				return err
			}
			chunkChan <- csvRow
		}
	}
	close(chunkChan)
	return nil
}

func SchemaLossless(row interface{}) ([]string, error) {

	// Reflect on the interface to discover its underlying type and value.
	val := reflect.ValueOf(row)

	// If the underlying value is a pointer, we need to get the value that the pointer points to.
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	// Make sure we now have a struct type after dereferencing any pointer.
	if val.Kind() != reflect.Struct {
		panic("Item is not a struct")
	}

	var csvRow []string
	for j := 0; j < val.NumField(); j++ {
		field := val.Field(j)
		// We need to check if the field is an exported field (capitalized name).
		if field.CanInterface() {
			switch field.Kind() {
			case reflect.String:
				csvRow = append(csvRow, field.String())
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				csvRow = append(csvRow, strconv.FormatInt(field.Int(), 10))
			case reflect.Float32, reflect.Float64:
				csvRow = append(csvRow, strconv.FormatFloat(field.Float(), 'f', -1, 64))
			case reflect.Bool:
				csvRow = append(csvRow, strconv.FormatBool(field.Bool()))
			// Add more cases as needed for other types.
			default:
				// If it's a pointer, we need to check if it's nil.
				if field.Kind() == reflect.Ptr && !field.IsNil() {
					// Recursively process the field by dereferencing the pointer.
					csvRow = append(csvRow, fmt.Sprintf("%v", field.Elem().Interface()))
				} else {
					// For all other types, use fmt.Sprintf to convert to a string.
					csvRow = append(csvRow, fmt.Sprintf("%v", field.Interface()))
				}
			}
		}

	}
	return csvRow, nil
}

// func ReadParquetInChunks(filePath string, chunkChan chan<- [][]string, chunkSize int) error {
// 	fr, err := local.NewLocalFileReader(filePath)
// 	if err != nil {
// 		return err
// 	}
// 	defer fr.Close()

// 	pr, err := reader.NewParquetReader(fr, nil, int64(chunkSize))
// 	if err != nil {
// 		return err
// 	}
// 	defer pr.ReadStop()

// 	numRows := int(pr.GetNumRows())
// 	// numCols := int(pr.SchemaHandler.GetColumnNum())

// 	for start := 0; start < numRows; start += chunkSize {
// 		end := start + chunkSize
// 		if end > numRows {
// 			end = numRows
// 		}

// 		rows := make([]ParquetRec, end-start)
// 		if err := pr.Read(&rows); err != nil {
// 			return fmt.Errorf("error at chunk %d: %v", start, err)
// 		}

// 		chunk := make([][]string, end-start)

// 		for i, row := range rows {

// 			val := reflect.ValueOf(row)

// 			if val.Kind() == reflect.Struct {

// 				var fields []string

// 				for i := 0; i < val.NumField(); i++ {
// 					field := val.Field(i)
// 					fields = append(fields, fmt.Sprintf("%v", field.Interface()))
// 				}
// 				chunk[i] = fields
// 			}

// 		}

// 		chunkChan <- chunk
// 	}

// 	close(chunkChan)
// 	return nil
// }
