// minimal version of hey.go
package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"time"
)

var (
	N   int     = 20 // total number of requests
	C   int     = 10 // concurrency level
	QPS float64 = 2  // queries per second
)

const endpoint = "https://google.com"

func main() {
	stopChan := make(chan struct{}, C)

	// capture the interrupt signal
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)

	go func() {
		<-signalChan

		for range C {
			stopChan <- struct{}{}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(C)

	client := &http.Client{
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// disable redirection and returns latest response
			return http.ErrUseLastResponse
		},
	}

	// control the timing of initiating the request
	throttle := time.Tick(time.Duration(1e6/QPS) * time.Microsecond)

	var reqID atomic.Int64

	for range C {
		go func() {
			defer wg.Done()

			for range N / C {
				select {
				case <-stopChan:
					return
				default:
					<-throttle
					makeRequest(client, reqID.Add(1))
				}
			}
		}()
	}

	wg.Wait()
}

func makeRequest(c *http.Client, id int64) error {
	req, err := http.NewRequest(http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	// TODO: trace the request
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode < 400 {
		log.Printf("request [%d] successfully reached %s", id, endpoint)
	}
	return nil
}
