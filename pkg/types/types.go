package types

import (
	"encoding/json"
	"time"
)

type SortOrder string

const (
	SortOrderAscending  SortOrder = "asc"
	SortOrderDescending SortOrder = "desc"
)

type SearchBody struct {
	Query        *SearchQuery          `json:"query,omitempty"`
	Aggregations *Aggregations         `json:"aggs,omitempty"`
	Slice        *Slice                `json:"slice,omitempty"`
	Sort         []map[string]SortSpec `json:"sort,omitempty"`
	SearchAfter  []any                 `json:"search_after,omitempty"`
}

type SortSpec struct {
	Order SortOrder `json:"order,omitempty"`
}

type Slice struct {
	ID  int `json:"id"`
	Max int `json:"max"`
}

type SearchQuery struct {
	Bool BoolQuery `json:"bool,omitempty"`
}

type BoolQuery struct {
	Must    []GenericQuery `json:"must,omitempty"`
	MustNot []GenericQuery `json:"must_not,omitempty"`
	Sbould  []GenericQuery `json:"should,omitempty"`
	Filter  []GenericQuery `json:"filter,omitempty"`
}

type GenericQuery struct {
	Range map[string]interface{} `json:"range,omitempty"`
	Term  *TermQuery             `json:"term,omitempty"`
}

type RangeQueryTime struct {
	LessThanEqual    *time.Time `json:"lte,omitempty"`
	LessThan         *time.Time `json:"lt,omitempty"`
	GreaterThanEqual *time.Time `json:"gte,omitempty"`
	GreaterThan      *time.Time `json:"gt,omitempty"`
}

type TermQuery map[string]TermValue

type TermValue struct {
	Value string `json:"value,omitempty"`
}

type Aggregations struct {
	Aggregation map[string]GenericAggregation `json:"-"`
}

type GenericAggregation struct {
	Terms              *TermsAggregation `json:"terms,omitempty"`
	Min                *MinAggregation   `json:"min,omitempty"`
	Max                *MaxAggregation   `json:"max,omitempty"`
	NestedAggregations *Aggregations     `json:"aggs,omitempty"`
}

type TermsAggregation struct {
	Field string `json:"field,omitempty"`
	Size  int    `json:"size,omitempty"`
}

type MaxAggregation struct {
	Field string `json:"field,omitempty"`
}

type MinAggregation struct {
	Field string `json:"field,omitempty"`
}

func (a Aggregations) MarshalJSON() ([]byte, error) {
	type Aggregations_ Aggregations
	b, err := json.Marshal(Aggregations_(a))
	if err != nil {
		return nil, err
	}

	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}

	for k, v := range a.Aggregation {
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		m[k] = b
	}

	return json.Marshal(m)
}
