package http

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/netsampler/goflow2/v2/transport"
)

type HTTPDriver struct {
	httpDestination     string
	httpAuthHeader      string
	httpAuthCredentials string
	lock                *sync.RWMutex
	q                   chan bool
	batchSize           int
	batchData           []map[string]interface{}
	timerDuration       time.Duration
	timer               *time.Timer
}

func (d *HTTPDriver) Prepare() error {
	defaultBatchSize := 1000
	defaultTimerDuration := 5

	flag.StringVar(&d.httpDestination, "transport.http.destination", "", "HTTP endpoint for output")
	flag.StringVar(&d.httpAuthHeader, "transport.http.auth.header", "", "HTTP header to set for credentials")
	flag.StringVar(&d.httpAuthCredentials, "transport.http.auth.credentials", "", "credentials for the header")
	flag.IntVar(&d.batchSize, "transport.http.batchSize", defaultBatchSize, fmt.Sprintf("Batch size for sending records, default %d", defaultBatchSize))
	batchExpireTimer := flag.Int("transport.http.timerDuration", defaultTimerDuration, fmt.Sprintf("Duration for the timer to send batch data, default %d", defaultTimerDuration))

	if *batchExpireTimer <= 0 {
		d.timerDuration = time.Duration(defaultTimerDuration) * time.Second
	} else {
		d.timerDuration = time.Duration(*batchExpireTimer) * time.Second
	}

	if d.batchSize <= 0 {
		d.batchSize = defaultBatchSize // default batch size
	}

	return nil
}

func (d *HTTPDriver) Init() error {
	d.q = make(chan bool, 1)
	d.batchData = make([]map[string]interface{}, 0, d.batchSize)
	d.timer = time.NewTimer(d.timerDuration)

	go func() {
		for {
			select {
			case <-d.timer.C:
				d.sendBatchData()
				d.timer.Reset(d.timerDuration)
			case <-d.q:
				return
			}
		}
	}()

	return nil
}

func (d *HTTPDriver) Send(key, data []byte) error {
	batchData := make(map[string]interface{})
	err := json.Unmarshal(data, &batchData)
	if err != nil {
		slog.Error("unmarshal json", slog.String("error", err.Error()))
		return err
	}
	d.batchData = append(d.batchData, batchData)
	if len(d.batchData) >= d.batchSize {
		d.sendBatchData()
		d.timer.Reset(d.timerDuration)
	}

	return nil
}

func (d *HTTPDriver) sendBatchData() {
	if len(d.batchData) == 0 {
		return
	}

	jsonData, err := json.Marshal(d.batchData)
	if err != nil {
		slog.Error("marshal batch data", slog.String("error", err.Error()))
		return
	}

	maxRetries := 3
	delay := time.Millisecond * 500 // initial delay

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < maxRetries; i++ {
			req, err := http.NewRequest("POST", d.httpDestination, bytes.NewBuffer(jsonData))
			if err != nil {
				slog.Error("create http request", slog.String("error", err.Error()))
				return
			}

			req.Header.Set("Content-Type", "application/json")
			if d.httpAuthHeader != "" && d.httpAuthCredentials != "" {
				req.Header.Set(d.httpAuthHeader, d.httpAuthCredentials)
			}

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil || (resp.StatusCode < 200 || resp.StatusCode >= 300) {
				if i == maxRetries-1 {
					slog.Error("request", slog.String("error", err.Error()))

					return
				}
				time.Sleep(delay * time.Duration(math.Pow(2, float64(i)))) // exponential backoff
				continue
			}
			defer resp.Body.Close()

			// reset batchData
			d.batchData = d.batchData[:0]
			break
		}
	}()

	wg.Wait()
}

func (d *HTTPDriver) Close() error {
	close(d.q)
	return nil
}

func init() {
	d := &HTTPDriver{
		lock: &sync.RWMutex{},
	}
	transport.RegisterTransportDriver("http", d)
}
