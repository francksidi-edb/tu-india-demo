package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/google/uuid"
)

const kafkaBroker = "localhost:9092"

var kafkaTopic = "ecommerce-orders"

// regionConfig holds everything that varies by locale: names, geography,
// payment methods, email domains, and per-category price ranges (in the
// region's local currency — INR for india, EGP for egypt). The Order schema
// itself has no currency column, so don't mix regions within a single run
// against the same topic/table — total_price would silently blend two
// different currencies. Pick one region per run.
type regionConfig struct {
	label          string // just for the startup log line
	categories     []string
	countries      []string // "country" field: Indian states / Egyptian governorates
	cities         []string
	firstNames     []string
	lastNames      []string
	emailProviders []string
	paymentMethods []string
	priceRange     map[string][2]float64 // category -> [min, max] in local currency
}

var regions = map[string]regionConfig{
	"india": {
		label: "India (INR)",
		categories: []string{"Electronics", "Clothing", "Books", "Home & Kitchen", "Sports", "Toys",
			"Mobiles & Accessories", "Grocery", "Beauty & Personal Care", "Ayurveda & Wellness"},
		countries: []string{"Maharashtra", "Delhi", "Karnataka", "Tamil Nadu", "Telangana",
			"West Bengal", "Gujarat", "Rajasthan", "Uttar Pradesh", "Kerala", "Punjab", "Haryana"},
		cities: []string{"Mumbai", "Pune", "New Delhi", "Bengaluru", "Chennai", "Hyderabad",
			"Kolkata", "Ahmedabad", "Surat", "Jaipur", "Lucknow", "Kochi", "Chandigarh", "Gurugram"},
		firstNames: []string{"Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Krishna",
			"Rohan", "Karthik", "Ananya", "Priya", "Diya", "Saanvi", "Aadhya", "Ishita",
			"Kavya", "Sneha", "Pooja", "Anjali", "Neha", "Riya"},
		lastNames: []string{"Sharma", "Verma", "Gupta", "Patel", "Reddy", "Nair", "Iyer", "Menon",
			"Singh", "Kumar", "Rao", "Chopra", "Mehta", "Joshi", "Desai", "Pillai", "Agarwal", "Bhat"},
		emailProviders: []string{"gmail.com", "yahoo.in", "outlook.com", "rediffmail.com"},
		paymentMethods: []string{"UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "Paytm Wallet", "EMI"},
		// Per-category unit-price ranges (INR). Spread by category is what
		// produces a natural mix across the low/medium/high/premium buckets
		// (fixed thresholds: <100 low, <500 medium, <1000 high, >=1000 premium).
		priceRange: map[string][2]float64{
			"Grocery":                {40, 1200},
			"Books":                  {99, 1500},
			"Beauty & Personal Care": {99, 2500},
			"Ayurveda & Wellness":    {99, 2200},
			"Toys":                   {150, 3000},
			"Sports":                 {200, 6000},
			"Home & Kitchen":         {200, 9000},
			"Clothing":               {300, 5000},
			"Mobiles & Accessories":  {500, 45000},
			"Electronics":            {500, 85000},
		},
	},
	"egypt": {
		label: "Egypt (EGP)",
		categories: []string{"Electronics", "Clothing", "Books", "Home & Kitchen", "Sports", "Toys",
			"Mobiles & Accessories", "Grocery", "Beauty & Personal Care", "Health & Wellness"},
		countries: []string{"Cairo", "Giza", "Alexandria", "Qalyubia", "Sharqia", "Dakahlia",
			"Gharbia", "Beheira", "Minya", "Assiut", "Suez", "Ismailia", "Port Said",
			"Faiyum", "Beni Suef", "Aswan", "Luxor", "Red Sea", "Sohag", "Kafr El Sheikh"},
		cities: []string{"Cairo", "Giza", "Alexandria", "Mansoura", "Tanta", "Asyut", "Ismailia",
			"Suez", "Luxor", "Aswan", "Damietta", "Zagazig", "Shubra El-Kheima",
			"6th of October City", "New Cairo", "Faiyum", "Beni Suef", "Minya", "Port Said", "Hurghada"},
		firstNames: []string{"Ahmed", "Mohamed", "Mahmoud", "Omar", "Youssef", "Karim", "Hassan", "Ali",
			"Khaled", "Amr", "Mostafa", "Tarek", "Sara", "Mona", "Nourhan", "Yasmin",
			"Aya", "Salma", "Farida", "Nadine", "Heba", "Dina", "Rania", "Mariam"},
		lastNames: []string{"El-Sayed", "Abdelrahman", "Hassan", "Mahmoud", "Ibrahim", "Farouk",
			"El-Masry", "Shafik", "Mansour", "Kamal", "Fathy", "Aziz", "Naguib",
			"Saleh", "Zaki", "Gaber", "Youssef", "Adel"},
		emailProviders: []string{"gmail.com", "yahoo.com", "hotmail.com", "outlook.com"},
		paymentMethods: []string{"Cash on Delivery", "Visa/Mastercard", "Meeza Card", "Vodafone Cash", "Fawry", "InstaPay", "Orange Money"},
		// Per-category unit-price ranges (EGP). Same target bucket spread as
		// the India ranges, against the same fixed 100/500/1000 thresholds.
		priceRange: map[string][2]float64{
			"Grocery":                {20, 800},
			"Books":                  {60, 1200},
			"Beauty & Personal Care": {60, 2000},
			"Health & Wellness":      {60, 1800},
			"Toys":                   {100, 2500},
			"Sports":                 {150, 5000},
			"Home & Kitchen":         {150, 7000},
			"Clothing":               {200, 4000},
			"Mobiles & Accessories":  {400, 35000},
			"Electronics":            {400, 70000},
		},
	},
}

type Order struct {
	OrderID       string  `json:"order_id"`
	Timestamp     string  `json:"timestamp"`
	CustomerID    string  `json:"customer_id"`
	CustomerName  string  `json:"customer_name"`
	CustomerEmail string  `json:"customer_email"`
	ProductID     string  `json:"product_id"`
	ProductName   string  `json:"product_name"`
	Category      string  `json:"category"`
	Quantity      int     `json:"quantity"`
	UnitPrice     float64 `json:"unit_price"`
	TotalPrice    float64 `json:"total_price"`
	PaymentMethod string  `json:"payment_method"`
	Country       string  `json:"country"`
	City          string  `json:"city"`
}

// weightedQuantity biases toward small orders (like real e-commerce), with
// occasional bulk orders — instead of a flat 1-5 draw that overweights bulk.
func weightedQuantity(rng *rand.Rand) int {
	roll := rng.Float64()
	switch {
	case roll < 0.55:
		return 1
	case roll < 0.80:
		return 2
	case roll < 0.93:
		return 3
	case roll < 0.98:
		return 4
	default:
		return 5
	}
}

func round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func generateOrder(rng *rand.Rand, cfg *regionConfig) Order {
	category := cfg.categories[rng.Intn(len(cfg.categories))]
	priceRange := cfg.priceRange[category]
	minP, maxP := priceRange[0], priceRange[1]
	unitPrice := round2(minP + rng.Float64()*(maxP-minP))

	quantity := weightedQuantity(rng)

	firstName := cfg.firstNames[rng.Intn(len(cfg.firstNames))]
	lastName := cfg.lastNames[rng.Intn(len(cfg.lastNames))]
	provider := cfg.emailProviders[rng.Intn(len(cfg.emailProviders))]
	return Order{
		OrderID:       fmt.Sprintf("ORD-%s", uuid.New().String()[:8]),
		Timestamp:     time.Now().Format("2006-01-02 15:04:05.000"),
		CustomerID:    fmt.Sprintf("CUST-%05d", rng.Intn(1000)),
		CustomerName:  fmt.Sprintf("%s %s", firstName, lastName),
		CustomerEmail: fmt.Sprintf("%s.%s@%s", firstName, lastName, provider),
		ProductID:     fmt.Sprintf("PROD-%04d", rng.Intn(500)),
		ProductName:   fmt.Sprintf("Product %d", rng.Intn(100)),
		Category:      category,
		Quantity:      quantity,
		UnitPrice:     unitPrice,
		TotalPrice:    round2(float64(quantity) * unitPrice),
		PaymentMethod: cfg.paymentMethods[rng.Intn(len(cfg.paymentMethods))],
		Country:       cfg.countries[rng.Intn(len(cfg.countries))],
		City:          cfg.cities[rng.Intn(len(cfg.cities))],
	}
}

func main() {
	rate := flag.Int("rate", 10000, "Target orders per second (total across all workers)")
	workers := flag.Int("workers", 4, "Number of parallel producer goroutines")
	maxMessages := flag.Int("max-messages", 0, "Maximum total messages (0=unlimited)")
	batchSize := flag.Int("batch", 500, "Kafka producer batch size")
	region := flag.String("region", "india", "Customer locale: india or egypt")
	flag.Parse()

	regionKey := strings.ToLower(strings.TrimSpace(*region))
	cfg, ok := regions[regionKey]
	if !ok {
		valid := make([]string, 0, len(regions))
		for k := range regions {
			valid = append(valid, k)
		}
		log.Fatalf("Unknown --region %q — valid options: %v", *region, valid)
	}

	log.Printf("Starting order generator: region=%s (%s)  target=%d/s  workers=%d  batch=%d  max=%d",
		regionKey, cfg.label, *rate, *workers, *batchSize, *maxMessages)

	var totalSent int64
	var totalErr int64

	ratePerWorker := *rate / *workers
	if ratePerWorker < 1 {
		ratePerWorker = 1
	}

	stop := make(chan struct{})
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() { <-sigs; close(stop) }()

	var wg sync.WaitGroup

	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			p, err := kafka.NewProducer(&kafka.ConfigMap{
				"bootstrap.servers":            kafkaBroker,
				"queue.buffering.max.messages": 1000000,
				"queue.buffering.max.kbytes":   1048576,
				"batch.num.messages":           *batchSize,
				"linger.ms":                    5,
				"compression.type":             "lz4",
				"acks":                         1,
			})
			if err != nil {
				log.Fatalf("Worker %d: failed to create producer: %s", workerID, err)
			}
			defer p.Close()

			go func() {
				for e := range p.Events() {
					if m, ok := e.(*kafka.Message); ok && m.TopicPartition.Error != nil {
						atomic.AddInt64(&totalErr, 1)
					}
				}
			}()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*1000))
			interval := time.Second / time.Duration(ratePerWorker)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			local := 0

			for {
				select {
				case <-stop:
					p.Flush(5000)
					return
				case <-ticker.C:
					if *maxMessages > 0 && int(atomic.LoadInt64(&totalSent)) >= *maxMessages {
						p.Flush(5000)
						return
					}
					order, _ := json.Marshal(generateOrder(rng, &cfg))
					err := p.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
						Value:          order,
					}, nil)
					if err != nil {
						atomic.AddInt64(&totalErr, 1)
					} else {
						atomic.AddInt64(&totalSent, 1)
						local++
						if local%(*batchSize) == 0 {
							p.Flush(100)
						}
					}
				}
			}
		}(w)
	}

	// Progress reporter every 5s
	go func() {
		prev := int64(0)
		start := time.Now()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				now := atomic.LoadInt64(&totalSent)
				errs := atomic.LoadInt64(&totalErr)
				delta := now - prev
				prev = now
				avg := float64(now) / time.Since(start).Seconds()
				log.Printf("sent=%d  last5s=%d  avg=%.0f/s  errors=%d", now, delta, avg, errs)
			}
		}
	}()

	wg.Wait()
	log.Printf("Done. Total sent=%d  errors=%d", atomic.LoadInt64(&totalSent), atomic.LoadInt64(&totalErr))
}


