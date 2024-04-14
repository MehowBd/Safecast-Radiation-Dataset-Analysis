package main

import (
	"encoding/csv"
	"fmt"
	"os"
)

func writeCSV(data [][]string, headers []string, directoryPath string, fileName string) error {
  if _, err := os.Stat(directoryPath); os.IsNotExist(err) {
    if err := os.MkdirAll(directoryPath, 0755); err != nil { // Create the directory if it does not exist
      logError("Failed to create directory: %v", err)
      return err
    }
  }

  filePath := fmt.Sprintf("%s/%s.csv", dataDirectory, fileName)
  file, err := os.Create(filePath)
  if err != nil {
    logError("Failed to create file: %v", err)
    return err
  }
  defer file.Close()

  writer := csv.NewWriter(file)
  defer writer.Flush()

  if err := writer.Write(headers); err != nil {
    logError("Failed to write headers to CSV: %v", err)
    return err
  }

  for _, record := range data {
    if err := writer.Write(record); err != nil {
      logError("Failed to write record to CSV: %v", err)
      return err
    }
  }

  logSuccess("Results saved to %s.", filePath)
  return nil
}
