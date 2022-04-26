package main

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/dbason/opni-support-fetcher/pkg/publish"
	"github.com/dbason/opni-support-fetcher/pkg/queue"
	"github.com/dbason/opni-support-fetcher/pkg/search"
	"github.com/dbason/opni-support-fetcher/pkg/util"
	"github.com/nats-io/nats.go"
	"github.com/opensearch-project/opensearch-go"
)

var (
	osClient *opensearch.Client
	nc       *nats.Conn
)

func main() {
	var err error
	opensearchURL := os.Getenv("ES_ENDPOINT")
	opensearchUser := os.Getenv("ES_USERNAME")
	opensearchPassword := os.Getenv("ES_PASSWORD")

	util.Log.Infof("connecting to opensearch: %s", opensearchURL)

	// Set sane transport timeouts
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Dial = (&net.Dialer{
		Timeout: 5 * time.Second,
	}).Dial
	transport.TLSHandshakeTimeout = 5 * time.Second
	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	osCfg := opensearch.Config{
		Addresses: []string{
			opensearchURL,
		},
		Username:             opensearchUser,
		Password:             opensearchPassword,
		UseResponseCheckOnly: true,
		Transport:            transport,
	}

	osClient, err = opensearch.NewClient(osCfg)
	if err != nil {
		util.Log.Fatalf("failed to set up Opensearch client: %v", err)
	}

	natsURL := os.Getenv("NATS_SERVER_URL")
	natsSeedPath := os.Getenv("NKEY_SEED_FILENAME")

	util.Log.Infof("connecting to NATS: %s", natsURL)

	opt, err := nats.NkeyOptionFromSeed(natsSeedPath)
	if err != nil {
		util.Log.Fatalf("failed to set up NATS auth: %v", err)
	}

	retryBackoff := backoff.NewExponentialBackOff()

	nc, err = nats.Connect(
		natsURL,
		opt,
		nats.MaxReconnects(-1),
		nats.CustomReconnectDelay(
			func(i int) time.Duration {
				if i == 1 {
					retryBackoff.Reset()
				}
				return retryBackoff.NextBackOff()
			},
		),
		nats.DisconnectErrHandler(
			func(nc *nats.Conn, err error) {
				util.Log.Errorf("nats disconnected: %s", err)
			},
		),
	)
	if err != nil {
		util.Log.Fatalf("failed to set up NATS client: %v", err)
	}

	ctx, _ := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	getPending(ctx)
}

func getPending(ctx context.Context) {
	util.Log.Debug("starting ticker")
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			fetcher := queue.NewQueueFetcher("pending-cases", osClient)
			publisher, hits := publish.NewPublisher(4, nc)
			go publisher.Run(ctx)

			queueHandler := search.NewSearcher(2, hits, osClient)
			jobs, err := fetcher.FetchItems(ctx)
			if err != nil {
				util.Log.Errorf("failed to fetch queue items: %v", err)
				continue
			}
			util.Log.Debugf("queueing %d jobs", len(jobs))
			go queueHandler.Generate(jobs)
			go queueHandler.Run(ctx)
		PUBLISH:
			for {
				select {
				case result, ok := <-queueHandler.Results():
					if !ok {
						continue
					}
					if result.Err != nil {
						util.Log.Errorf("job handler error: %v", result.Err)
						continue
					}
					if result.Params != nil {
						err := fetcher.DeleteItem(ctx, *result.Params)
						if err != nil {
							util.Log.Errorf("failed to delete queue item from index: %v", err)
						}
					}
				case <-queueHandler.Done:
					// All publishes are queued so close the channel and wait for it to be done
					close(hits)
					<-publisher.Done
					break PUBLISH
				default:
				}
			}
		case <-ctx.Done():
			return
		}
	}
}
