package publish

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dbason/opni-support-fetcher/pkg/util"
	"github.com/go-gota/gota/dataframe"
	"github.com/nats-io/nats.go"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Publisher struct {
	*nats.Conn
	workers int
	hits    chan gjson.Result
	Done    chan struct{}
}

func NewPublisher(
	workers int,
	nc *nats.Conn,
) (*Publisher, chan<- gjson.Result) {
	hits := make(chan gjson.Result)
	return &Publisher{
		Conn:    nc,
		workers: workers,
		hits:    hits,
		Done:    make(chan struct{}),
	}, hits
}

func (p *Publisher) Run(ctx context.Context) {
	wg := &sync.WaitGroup{}

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go p.doWork(ctx, wg, i)
	}

	wg.Wait()
	close(p.Done)
}

func (p *Publisher) doWork(ctx context.Context, wg *sync.WaitGroup, worker int) {
	defer wg.Done()

	for {
		select {
		case hitSlice, ok := <-p.hits:
			if !ok {
				util.Log.Debugf("hits channel closed, stopping publisher %d", worker)
				return
			}
			if err := p.publishResults(hitSlice); err != nil {
				util.Log.Errorf("publisher %d failed to publish logs: %v", err)
			}

		case <-ctx.Done():
			util.Log.Infof("execution cancelled, stopping publisher %d: %v", worker, ctx.Err())
			return
		}
	}
}

func (p *Publisher) publishResults(hits gjson.Result) error {
	var results []map[string]interface{}
	hits.ForEach(func(key, value gjson.Result) bool {
		id := value.Get("_id").String()
		source := value.Get("_source")
		jsonSource, err := sjson.Set(source.Raw, "processed", true)
		if err != nil {
			util.Log.Errorf("failed to update json: %v", err)
			return true
		}
		row := make(map[string]interface{})
		err = json.Unmarshal([]byte(jsonSource), &row)
		if err != nil {
			util.Log.Error("failed to extract source from document")
			return true
		}
		row["_id"] = id
		results = append(results, row)
		return true
	})

	df := dataframe.LoadMaps(results)
	data := convertDataframeToJSON(df)
	return p.Publish("raw_logs", data)
}

func convertDataframeToJSON(df dataframe.DataFrame) []byte {
	dfJson := make(map[string]interface{})
	for _, column := range df.Names() {
		series := df.Col(column)
		seriesMap := make(map[string]string)
		for i, value := range series.Records() {
			seriesMap[fmt.Sprintf("%d", i)] = value
		}
		dfJson[column] = seriesMap
	}

	marshalled, _ := json.Marshal(dfJson)
	return marshalled
}
