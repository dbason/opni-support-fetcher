package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dbason/opni-support-fetcher/pkg/search"
	"github.com/dbason/opni-support-fetcher/pkg/types"
	"github.com/dbason/opni-support-fetcher/pkg/util"
	supportpublish "github.com/dbason/opni-supportagent/pkg/publish"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	"github.com/tidwall/gjson"
	"k8s.io/utils/pointer"
)

type QueueFetcher struct {
	queueIndex string
	*opensearch.Client
}

func NewQueueFetcher(queueIndex string, client *opensearch.Client) QueueFetcher {
	return QueueFetcher{
		queueIndex: queueIndex,
		Client:     client,
	}
}

func (f *QueueFetcher) FetchItems(ctx context.Context) ([]search.SearchJob, error) {
	var jobs []search.SearchJob
	sleepTime := time.Now().Add(-5 * time.Minute)
	query := types.SearchBody{
		Query: &types.SearchQuery{
			Bool: types.BoolQuery{
				Must: []types.GenericQuery{
					{
						Range: map[string]interface{}{
							"sleep": types.RangeQueryTime{
								LessThanEqual: &sleepTime,
							},
						},
					},
				},
			},
		},
		Aggregations: &types.Aggregations{
			Aggregation: map[string]types.GenericAggregation{
				"cases": {
					Terms: &types.TermsAggregation{
						Field: "case.keyword",
						Size:  100,
					},
					NestedAggregations: &types.Aggregations{
						Aggregation: map[string]types.GenericAggregation{
							"start": {
								Min: &types.MinAggregation{
									Field: "start",
								},
							},
							"end": {
								Max: &types.MaxAggregation{
									Field: "end",
								},
							},
							"count": {
								Max: &types.MaxAggregation{
									Field: "count",
								},
							},
						},
					},
				},
			},
		},
	}

	req := opensearchapi.SearchRequest{
		Index: []string{
			f.queueIndex,
		},
		Body: opensearchutil.NewJSONReader(query),
		Size: pointer.Int(0),
	}

	resp, err := req.Do(ctx, f)
	if err != nil {
		return jobs, err
	}

	body := util.ReadString(resp.Body)
	resp.Body.Close()

	docCount := gjson.Get(body, "hits.total.value").Int()
	hits := gjson.Get(body, "aggregations.cases.buckets")
	if docCount < 1 {
		util.Log.Info("no queue items found")
		util.Log.Debugf("query is: %s", util.MustMarshal(query))
		return jobs, nil
	}

	jobs = append(jobs, parseHits(hits)...)

	return jobs, nil
}

func (f *QueueFetcher) DeleteItem(ctx context.Context, params supportpublish.SupportFetcherDoc) error {
	query := types.SearchBody{
		Query: &types.SearchQuery{
			Bool: types.BoolQuery{
				Filter: []types.GenericQuery{
					{
						Term: &types.TermQuery{
							"case.keyword": types.TermValue{
								Value: params.Case,
							},
						},
					},
				},
			},
		},
	}

	searchReq := opensearchapi.SearchRequest{
		Index: []string{
			f.queueIndex,
		},
		Body: opensearchutil.NewJSONReader(query),
		Size: pointer.Int(1000),
	}
	resp, err := searchReq.Do(ctx, f)
	if err != nil {
		return err
	}

	jsonBody := util.ReadString(resp.Body)
	resp.Body.Close()
	hits := gjson.Get(jsonBody, "hits.hits")

	if len(hits.Array()) < 1 {
		util.Log.Warnf("no queue item for case %s, adding back in to index", params.Case)
		req := opensearchapi.IndexRequest{
			Index: f.queueIndex,
			Body:  opensearchutil.NewJSONReader(params),
		}

		resp, err := req.Do(ctx, f)
		if err != nil {
			return err
		}
		defer resp.Body.Close()

		if resp.IsError() {
			return fmt.Errorf("failed to add pending cases doc: %s", resp.String())
		}
		return nil
	}

	hits.ForEach(func(key, value gjson.Result) bool {
		params := supportpublish.SupportFetcherDoc{}
		err := json.Unmarshal([]byte(value.Get("_source").Raw), &params)
		if err != nil {
			util.Log.Errorf("failed to unmarshal queue doc: %v", err)
			return false
		}
		id := value.Get("_id").String()
		count := f.logsProcessedCount(ctx, params)
		if count < 1 {
			util.Log.Debug("deleting queue item")
			resp, err := f.Delete(
				f.queueIndex,
				id,
				f.Delete.WithContext(ctx),
			)
			if err != nil {
				util.Log.Errorf("failed to delete queue item: %v", err)
				return true
			}
			defer resp.Body.Close()
			if resp.IsError() {
				util.Log.Errorf("failed to delete queue item: %s", resp.String())
			}
		} else {
			f.updateCountAndSleep(ctx, params, count, id)
		}
		return true
	})

	return nil
}

func (f *QueueFetcher) updateCountAndSleep(
	ctx context.Context,
	params supportpublish.SupportFetcherDoc,
	count int,
	id string,
) {
	params.Sleep = time.Now()
	params.Count = count
	body := map[string]any{
		"doc": params,
	}
	util.Log.Debug("unprocessed cases so updating sleep time and count")
	resp, err := f.Update(
		f.queueIndex,
		id,
		opensearchutil.NewJSONReader(body),
		f.Update.WithContext(ctx),
		f.Update.WithRefresh("true"),
	)
	if err != nil {
		util.Log.Errorf("failed to update sleep: %v", err)
		return
	}
	defer resp.Body.Close()
	if resp.IsError() {
		util.Log.Errorf("failed to update sleep: %s", resp.String())
	}
}

func (f *QueueFetcher) logsProcessedCount(ctx context.Context, params supportpublish.SupportFetcherDoc) int {
	util.Log.Debugf(
		"checking queue item case: %s, start: %s, end: %s",
		params.Case,
		params.Start.Format(time.RFC3339Nano),
		params.End.Format(time.RFC3339Nano),
	)

	query := types.SearchBody{
		Query: &types.SearchQuery{
			Bool: types.BoolQuery{
				Filter: []types.GenericQuery{
					{
						Term: &types.TermQuery{
							"cluster_id.keyword": types.TermValue{
								Value: params.Case,
							},
						},
					},
					{
						Term: &types.TermQuery{
							"agent": types.TermValue{
								Value: "support",
							},
						},
					},
					{
						Term: &types.TermQuery{
							"processed": types.TermValue{
								Value: "false",
							},
						},
					},
				},
				Must: []types.GenericQuery{
					{
						Range: map[string]interface{}{
							"time": types.RangeQueryTime{
								GreaterThanEqual: &params.Start,
								LessThanEqual:    &params.End,
							},
						},
					},
				},
			},
		},
	}

	req := opensearchapi.CountRequest{
		Index: []string{
			"logs",
		},
		Body: opensearchutil.NewJSONReader(query),
	}
	resp, err := req.Do(ctx, f)
	if err != nil {
		util.Log.Errorf("failed to count documents: %v", err)
		return 0
	}

	jsonBody := util.ReadString(resp.Body)
	resp.Body.Close()

	return int(gjson.Get(jsonBody, "count").Int())
}

func parseHits(hits gjson.Result) []search.SearchJob {
	var jobs []search.SearchJob
	hits.ForEach(func(key, value gjson.Result) bool {
		job := supportpublish.SupportFetcherDoc{
			Case: value.Get("key").String(),
		}

		start, err := time.Parse(time.RFC3339Nano, value.Get("start.value_as_string").String())
		if err != nil {
			util.Log.Errorf("failed to get start time for %s", job.Case)
			return true
		}
		job.Start = start

		end, err := time.Parse(time.RFC3339Nano, value.Get("end.value_as_string").String())
		if err != nil {
			util.Log.Errorf("failed to get start time for %s", job.Case)
			return true
		}
		job.End = end

		job.Count = int(value.Get("count.value").Int())

		jobs = append(jobs, search.SearchJob{
			Params: job,
		})
		return true
	})

	return jobs
}
