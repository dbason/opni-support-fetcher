package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/dbason/opni-support-fetcher/pkg/types"
	"github.com/dbason/opni-support-fetcher/pkg/util"
	supportpublish "github.com/dbason/opni-supportagent/pkg/publish"
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	"github.com/opensearch-project/opensearch-go/opensearchutil"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"k8s.io/utils/pointer"
)

type SearchResult struct {
	Params *supportpublish.SupportFetcherDoc
	Err    error
}

type SearchJob struct {
	Params supportpublish.SupportFetcherDoc
}

type Searcher struct {
	*opensearch.Client
	workers int
	publish chan<- gjson.Result
	jobs    chan SearchJob
	results chan SearchResult
	Done    chan struct{}
}

func NewSearcher(
	workers int,
	publish chan<- gjson.Result,
	osClient *opensearch.Client,
) *Searcher {
	return &Searcher{
		Client:  osClient,
		workers: workers,
		publish: publish,
		jobs:    make(chan SearchJob),
		results: make(chan SearchResult),
		Done:    make(chan struct{}),
	}
}

func (s *Searcher) fetchAndPublishLogs(ctx context.Context, params supportpublish.SupportFetcherDoc) error {
	size := 100

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
		Sort: []map[string]types.SortSpec{
			{
				"time": {
					Order: types.SortOrderAscending,
				},
			},
			{
				"_id": {
					Order: types.SortOrderDescending,
				},
			},
		},
	}

	req := opensearchapi.SearchRequest{
		Index: []string{
			"logs",
		},
		Body: opensearchutil.NewJSONReader(query),
		Size: pointer.Int(size),
	}

	resp, err := req.Do(ctx, s)
	if err != nil {
		return err
	}

	jsonBody := util.ReadString(resp.Body)
	resp.Body.Close()

	hits := gjson.Get(jsonBody, "hits.hits")

	count := len(hits.Array())

	if count < 1 {
		util.Log.Warn("found no logs for case %s", params.Case)
		util.Log.Debugf("query is: %s", util.MustMarshal(query))
		return nil
	}

	countQuery := types.SearchBody{
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

	countReq := opensearchapi.CountRequest{
		Index: []string{
			"logs",
		},
		Body: opensearchutil.NewJSONReader(countQuery),
	}

	resp, err = countReq.Do(ctx, s)
	if err != nil {
		util.Log.Errorf("failed to count documents: %v", err)
		return err
	}

	jsonBodyCount := util.ReadString(resp.Body)
	resp.Body.Close()

	docCount := int(gjson.Get(jsonBodyCount, "count").Int())

	util.Log.Debugf("stored count is %d, current count is %d", params.Count, docCount)

	if docCount < params.Count {
		util.Log.Infof("%s is still processing so not queueing any logs", params.Case)
		return nil
	}

	s.publish <- hits

	// error has already been checked in first opensearch query
	queryString := util.MustMarshal(query)

	for {
		jsonPath := fmt.Sprintf("hits.hits.%d.sort", size-1)
		searchAfter := gjson.Get(jsonBody, jsonPath)
		searchAfterQuery, err := sjson.SetRaw(queryString, "search_after", searchAfter.Raw)
		if err != nil {
			util.Log.Error("failed to set search_after")
			return err
		}

		req := opensearchapi.SearchRequest{
			Index: []string{
				"logs",
			},
			Body: strings.NewReader(searchAfterQuery),
			Size: pointer.Int(size),
		}

		resp, err := req.Do(ctx, s)
		if err != nil {
			return err
		}

		jsonBody = util.ReadString(resp.Body)
		resp.Body.Close()

		hits = gjson.Get(jsonBody, "hits.hits")
		newCount := len(hits.Array())
		count = count + newCount
		s.publish <- hits
		if newCount < size {
			util.Log.Debugf("finished processing logs for case %s", params.Case)
			break
		}
	}
	util.Log.Debugf("pushed %d logs to publisher", count)

	return nil
}
