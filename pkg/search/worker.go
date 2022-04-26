package search

import (
	"context"
	"sync"

	"github.com/dbason/opni-support-fetcher/pkg/util"
)

func (p *Searcher) doWork(ctx context.Context, wg *sync.WaitGroup, worker int) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-p.jobs:
			if !ok {
				util.Log.Debugf("jobs channel closed, stopping worker %d", worker)
				return
			}
			p.fetchAndPublishLogs(ctx, job.Params)
			p.results <- SearchResult{
				Params: &job.Params,
				Err:    nil,
			}
		case <-ctx.Done():
			util.Log.Infof("execution cancelled, stopping worker %d: %v", worker, ctx.Err())
			p.results <- SearchResult{
				Params: nil,
				Err:    ctx.Err(),
			}
			return
		}
	}
}

func (p *Searcher) Run(ctx context.Context) {
	wg := &sync.WaitGroup{}

	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		util.Log.Debugf("starting worker %d", i)
		go p.doWork(ctx, wg, i)
	}

	wg.Wait()
	close(p.Done)
	close(p.results)
}

func (p *Searcher) Generate(jobs []SearchJob) {
	for i := range jobs {
		util.Log.Debugf("queuing job case: %s", jobs[i].Params.Case)
		p.jobs <- jobs[i]
	}
	close(p.jobs)
}

func (p *Searcher) Results() <-chan SearchResult {
	return p.results
}
