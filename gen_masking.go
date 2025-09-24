package main

import (
	"math/rand"
	"runtime"
	"sync"
)

type MaskingService struct {
	mapMask (map[string]string)
	rwMutex sync.RWMutex
}

func NewMaskingService() *MaskingService {
	return &MaskingService{
		mapMask: make(map[string]string),
	}
}

var masking = NewMaskingService()

func (ms *MaskingService) maskValue(input string) string {

	ms.rwMutex.RLock()
	if masked, exists := ms.mapMask[input]; exists {
		ms.rwMutex.RUnlock()
		return masked
	}
	ms.rwMutex.RUnlock()

	var result []rune
	for _, ch := range input {
		if ch >= 'a' && ch <= 'z' {
			result = append(result, rune(rand.Intn(26)+'a'))
		} else if ch >= 'A' && ch <= 'Z' {
			result = append(result, rune(rand.Intn(26)+'A'))
		} else {
			result = append(result, ch)
		}
	}

	ms.rwMutex.Lock()
	ms.mapMask[input] = string(result)
	ms.rwMutex.Unlock()

	return string(result)
}

func MaskDataParallel(data []string, colIndexes []int) []string {
	maskedSlice := make([]string, len(colIndexes))

	for i, colIndex := range colIndexes {
		if colIndex < len(data) {
			maskedSlice[i] = masking.maskValue(data[colIndex])
		}
	}

	return maskedSlice
}

// MaskBatchParallel processes multiple rows in a batch for better performance
func MaskBatchParallel(batch [][]string, colIndexes []int) [][]string {
	result := make([][]string, len(batch))

	// Process each row in the batch
	for i, row := range batch {
		if row == nil {
			continue
		}

		// Create a copy of the row
		maskedRow := make([]string, len(row))
		copy(maskedRow, row)

		// Mask the specified columns
		for _, colIndex := range colIndexes {
			if colIndex < len(row) {
				maskedRow[colIndex] = masking.maskValue(row[colIndex])
			}
		}

		result[i] = maskedRow
	}

	return result
}

// maskRowWorker processes a range of rows for parallel masking
func maskRowWorker(batch [][]string, colIndexes []int, start, end int, result [][]string, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := start; j < end; j++ {
		row := batch[j]
		if row == nil {
			continue
		}

		// Create a copy of the row
		maskedRow := make([]string, len(row))
		copy(maskedRow, row)

		// Mask the specified columns
		for _, colIndex := range colIndexes {
			if colIndex < len(row) {
				maskedRow[colIndex] = masking.maskValue(row[colIndex])
			}
		}

		result[j] = maskedRow
	}
}

// calculateWorkerParams calculates optimal worker parameters for batch processing
func calculateWorkerParams(batchSize int) (numWorkers, chunkSize int) {
	numWorkers = runtime.GOMAXPROCS(0)
	if numWorkers > batchSize {
		numWorkers = batchSize
	}

	chunkSize = batchSize / numWorkers
	if chunkSize == 0 {
		chunkSize = 1
	}

	return numWorkers, chunkSize
}

// MaskBatchParallelWorkers processes batch using worker goroutines for very large batches
func MaskBatchParallelWorkers(batch [][]string, colIndexes []int) [][]string {
	if len(batch) < 100 { // For small batches, use simple processing
		return MaskBatchParallel(batch, colIndexes)
	}

	numWorkers, chunkSize := calculateWorkerParams(len(batch))
	result := make([][]string, len(batch))
	var wg sync.WaitGroup

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		start := i * chunkSize
		end := start + chunkSize
		if i == numWorkers-1 { // Last worker handles remainder
			end = len(batch)
		}

		go maskRowWorker(batch, colIndexes, start, end, result, &wg)
	}

	wg.Wait()
	return result
}
