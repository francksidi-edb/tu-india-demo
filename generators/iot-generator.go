package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const kafkaBroker = "localhost:9092"

var kafkaTopic = "iot-sensors-csv"

var (
	sensorIDs = []string{"SENS-001", "SENS-002", "SENS-003", "SENS-004", "SENS-005",
		"SENS-006", "SENS-007", "SENS-008", "SENS-009", "SENS-010"}

	buildings = []string{"Warehouse", "Parking", "Rooftop", "ColdStorage", "LoadingBay"}
	floors    = []int{1, 2, 3, 4, 5}

	statuses = []string{"normal", "high_temp", "low_temp", "high_humidity", "low_battery"}
)

type SensorReading struct {
	Timestamp    string
	SensorID     string
	Location     string
	Temperature  float64
	Humidity     float64
	Pressure     float64
	BatteryLevel float64
	Status       string
}

func (s SensorReading) ToCSV() string {
	return fmt.Sprintf("%s,%s,%s,%.2f,%.2f,%.2f,%.2f,%s",
		s.Timestamp, s.SensorID, s.Location, s.Temperature,
		s.Humidity, s.Pressure, s.BatteryLevel, s.Status)
}

func generateReading() SensorReading {
	sensorID := sensorIDs[rand.Intn(len(sensorIDs))]
	building := buildings[rand.Intn(len(buildings))]
	floor := floors[rand.Intn(len(floors))]
	location := fmt.Sprintf("%s-Floor-%d", building, floor)

	// Generate realistic sensor data for an Indian climate range
	temperature := 15.0 + rand.Float64()*30.0 // 15-45°C (Indian summers can be hot)
	humidity := 25.0 + rand.Float64()*65.0    // 25-90% (monsoon humidity)
	pressure := 980.0 + rand.Float64()*60.0   // 980-1040 hPa
	battery := 20.0 + rand.Float64()*80.0     // 20-100%

	// Determine status
	var status string
	if battery < 30 {
		status = "low_battery"
	} else if temperature > 35 {
		status = "high_temp"
	} else if temperature < 18 {
		status = "low_temp"
	} else if humidity > 75 {
		status = "high_humidity"
	} else {
		status = "normal"
	}

	return SensorReading{
		Timestamp:    time.Now().Format("2006-01-02 15:04:05"),
		SensorID:     sensorID,
		Location:     location,
		Temperature:  temperature,
		Humidity:     humidity,
		Pressure:     pressure,
		BatteryLevel: battery,
		Status:       status,
	}
}

func main() {
	rate := flag.Int("rate", 10000, "Readings per second")
	maxMessages := flag.Int("max-messages", 0, "Maximum messages to send (0=unlimited)")
	flag.Parse()

	log.Printf("Starting IoT sensor generator: %d readings/sec, max=%d", *rate, *maxMessages)

	// Create Kafka producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": kafkaBroker,
	})
	if err != nil {
		log.Fatalf("Failed to create producer: %s", err)
	}
	defer p.Close()

	// Delivery report handler
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v", ev.TopicPartition.Error)
				}
			}
		}
	}()

	// Send CSV header
	header := "timestamp,sensor_id,location,temperature,humidity,pressure,battery_level,status"
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          []byte(header),
	}, nil)
	if err != nil {
		log.Printf("Failed to send header: %s", err)
	}

	ticker := time.NewTicker(time.Second / time.Duration(*rate))
	defer ticker.Stop()

	count := 0
	for range ticker.C {
		if *maxMessages > 0 && count >= *maxMessages {
			log.Printf("Reached maximum messages (%d), stopping", *maxMessages)
			break
		}

		reading := generateReading()
		csvLine := reading.ToCSV()

		err := p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(csvLine),
		}, nil)

		if err != nil {
			log.Printf("Failed to produce message: %s", err)
		} else {
			count++
			if count%100 == 0 {
				log.Printf("Sent %d readings", count)
			}
		}
	}

	// Flush any pending messages
	p.Flush(15 * 1000)
	log.Printf("Generator stopped. Total readings sent: %d", count)
}

