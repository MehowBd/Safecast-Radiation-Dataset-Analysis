package main

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/joho/godotenv"
	_ "github.com/lib/pq"
)

const (
	filePrefix      = "measurements"
	dataDirectory   = "../data/chunks"
	retryLimit      = 3
	decreaseFactor  = 2
	minIncrement    = 1
	initialIncrement = 180
)

func init() {
	if err := godotenv.Load(); err != nil {
		logError("Error loading .env file")
	}
}

func connectDB() *sql.DB {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=require",
		os.Getenv("USER"), os.Getenv("PASSWORD"), os.Getenv("DATABASE"),
		os.Getenv("HOST"), os.Getenv("PORT"))

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		logError("Failed to connect to database: %v", err)
		return nil
	}
	logSuccess("Successfully connected to the database.")
	return db
}

func executeQuery(db *sql.DB, condition string, fileSuffix string) bool {
	query := fmt.Sprintf(`
		SELECT device_id, unit, location, height, captured_at::date AS measurement_day, AVG(value) AS average_value 
		FROM public.measurements 
		WHERE %s AND (device_id IS NOT NULL OR height IS NOT NULL)
		GROUP BY device_id, unit, location, height, measurement_day 
		ORDER BY measurement_day;`, 
		condition)
	rows, err := db.Query(query)
	if err != nil {
		logError("Failed to execute query: %v", err)
		return false
	}
	defer rows.Close()

	var data [][]string
	headers := []string{"Device ID", "Unit", "Location", "Height", "Measurement Day", "Average Value"}
	for rows.Next() {
		var (
			deviceID       sql.NullInt64
			unit, location sql.NullString
			height         sql.NullFloat64
			measurementDay sql.NullTime
			averageValue   sql.NullFloat64
		)
		if err := rows.Scan(&deviceID, &unit, &location, &height, &measurementDay, &averageValue); err != nil {
			logError("Failed to scan row: %v", err)
			return false
		}
		record := []string{
			nullInt64ToString(deviceID),
			nullStringToString(unit),
			nullStringToString(location),
			nullFloat64ToString(height),
			nullTimeToString(measurementDay),
			nullFloat64ToString(averageValue),
		}
		data = append(data, record)
	}

	if len(data) > 0 {
			var fileName = fmt.Sprintf("%s_%s.csv", filePrefix, fileSuffix)
			if err := writeCSV(data, headers, dataDirectory, fileName); err != nil {
					return false
			}
	} else {
			logInfo("No data found for the current query.")
	}

	return true
}

// Helper functions to convert SQL null types to strings
func nullStringToString(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}

func nullInt64ToString(ni sql.NullInt64) string {
	if ni.Valid {
		return fmt.Sprintf("%d", ni.Int64)
	}
	return ""
}

func nullFloat64ToString(nf sql.NullFloat64) string {
	if nf.Valid {
		return fmt.Sprintf("%.2f", nf.Float64)
	}
	return ""
}

func nullTimeToString(nt sql.NullTime) string {
	if nt.Valid {
		return nt.Time.Format("2006-01-02")
	}
	return ""
}


func main() {
	db := connectDB()
	if db == nil {
		return
	}
	defer db.Close()

	startDate := time.Date(2010, 1, 1, 0, 0, 0, 0, time.UTC)
	currentDate := startDate
	interval := initialIncrement

	for currentDate.Before(time.Now()) {
		nextDate := currentDate.AddDate(0, 0, interval)
		condition := fmt.Sprintf("captured_at BETWEEN '%s' AND '%s'", currentDate.Format("2006-01-02"), nextDate.Format("2006-01-02"))
		fileSuffix := fmt.Sprintf("%s_to_%s", currentDate.Format("2006-01-02"), nextDate.Format("2006-01-02"))

		logInfo("Executing query for interval: %s to %s", currentDate.Format("2006-01-02"), nextDate.Format("2006-01-02"))

		retries := retryLimit
		for retries > 0 {
			if executeQuery(db, condition, fileSuffix) {
				break
			}
			retries--
			logWarning("Retrying... %d retries left.", retries)
			if retries == 0 {
				if interval > minIncrement {
					interval = max(interval/decreaseFactor, minIncrement)
					logWarning("Reducing interval due to errors. New interval: %d", interval)
					retries = retryLimit
				} else {
					logError("Failed to execute query after multiple retries at minimum interval. Exiting loop.")
					return
				}
			}
		}
		currentDate = nextDate
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
