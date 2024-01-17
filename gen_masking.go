package main

import (
	"math/rand"
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

	maskedSlice := make([]string, len(data))

	for ind := range colIndexes {
		rowToMask := data[ind]
		maskedOut := masking.maskValue(rowToMask)
		// fmt.Println(maskedOut)
		maskedSlice = append(maskedSlice, maskedOut)
	}

	return maskedSlice

}

// func (ms *MaskingService) maskChunk(data []string, colIndexes []int, start, end int, resultChan chan<- [][]string, wg *sync.WaitGroup) {
// 	defer wg.Done()

// 	maskedChunk := make([][]string, end-start)
// 	for i := start; i < end; i++ {
// 		row := data[i]
// 		maskedRow := make([]string, len(row))
// 		copy(maskedRow, row)
// 		for _, colIndex := range colIndexes {
// 			maskedRow[colIndex] = masking.maskValue(row[colIndex])
// 		}
// 		maskedChunk[i-start] = maskedRow
// 	}
// 	resultChan <- maskedChunk
// }

// func MaskDataParallel(data []string, colIndexes []int) [][]string {
// 	const chunkSize = 50 // Adjust the chunk size based on your needs
// 	numChunks := (len(data) + chunkSize - 1) / chunkSize

// 	resultChan := make(chan [][]string, numChunks)
// 	var wg sync.WaitGroup

// 	for i := 0; i < numChunks; i++ {
// 		start := i * chunkSize
// 		end := start + chunkSize
// 		if end > len(data) {
// 			end = len(data)
// 		}
// 		wg.Add(1)
// 		go masking.maskChunk(data, colIndexes, start, end, resultChan, &wg)
// 	}

// 	wg.Wait()
// 	close(resultChan)

// 	maskedData := make([][]string, 0)
// 	for chunk := range resultChan {
// 		maskedData = append(maskedData, chunk...)
// 	}
// 	return maskedData
// }
