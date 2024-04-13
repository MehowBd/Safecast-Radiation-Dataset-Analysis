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
	dataDirectory   = "temp"
	retryLimit      = 3
	decreaseFactor  = 2
	minIncrement    = 1
	initialIncrement = 180
)

func init() {
	if err := godotenv.Load(); err != nil {
		Error("Error loading .env file")
	}
}

func connectDB() *sql.DB {
	connStr := fmt.Sprintf("user=%s password=%s dbname=%s host=%s port=%s sslmode=require",
		os.Getenv("USER"), os.Getenv("PASSWORD"), os.Getenv("DATABASE"),
		os.Getenv("HOST"), os.Getenv("PORT"))

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		Error("Failed to connect to database: %v", err)
		return nil
	}
	Info("Successfully connected to the database.")
	return db
}

func executeQuery(db *sql.DB, condition string, fileSuffix string) bool {
	query := fmt.Sprintf(`SELECT device_id, unit, location, height, captured_at::date AS measurement_day, AVG(value) AS average_value FROM public.measurements WHERE %s GROUP BY device_id, unit, location, height, measurement_day ORDER BY device_id;`, condition)
	rows, err := db.Query(query)
	if err != nil {
		Error("Failed to execute query: %v", err)
		return false
	}
	defer rows.Close()

	if rows.Next() {
		fileName := fmt.Sprintf("%s/%s_%s.csv", dataDirectory, filePrefix, fileSuffix)
		Info("Results saved to %s.", fileName)
		return true
	} else {
		Info("No data to save.")
		return true
	}
}

func main() {
	startDate := time.Date(2014, 12, 6, 0, 0, 0, 0, time.UTC)
	interval := initialIncrement
	currentDate := startDate

	db := connectDB()
	defer db.Close()

	for currentDate.Before(time.Now()) {
		nextDate := currentDate.AddDate(0, 0, interval)
		condition := fmt.Sprintf("captured_at BETWEEN '%s' AND '%s'", currentDate.Format("2006-01-02"), nextDate.Format("2006-01-02"))
		fileSuffix := fmt.Sprintf("%s_to_%s", currentDate.Format("2006-01-02"), nextDate.Format("2006-01-02"))

		retries := retryLimit
		for retries > 0 {
			if executeQuery(db, condition, fileSuffix) {
				break
			}
			retries--
			Warn("Retrying... %d retries left.", retries)
			if retries == 0 && interval > minIncrement {
				interval = max(interval/decreaseFactor, minIncrement)
				Warn("Reducing interval due to errors. New interval: %d", interval)
				retries = retryLimit
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
