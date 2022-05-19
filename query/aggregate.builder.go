package query

import (
	"errors"
	"fmt"
	"regexp"

	"duolacloud.com/duolacloud/crud-core/types"
	"go.mongodb.org/mongo-driver/bson"
)

type AggregateFunc string

const (
	AggregateFuncAVG   AggregateFunc = "avg"
	AggregateFuncSUM   AggregateFunc = "sum"
	AggregateFuncCOUNT AggregateFunc = "count"
	AggregateFuncMAX   AggregateFunc = "max"
	AggregateFuncMIN   AggregateFunc = "min"
)

type AggregateBuilder struct {
}

var AGG_REGEXP = regexp.MustCompile("(avg|sum|count|max|min|group_by)_(.*)")

func NewAggregateBuilder() *AggregateBuilder {
	return &AggregateBuilder{}
}

func (b *AggregateBuilder) build(aggregate *types.AggregateQuery) (bson.M, error) {
	aggSelect := make(bson.M)

	aggSelect2 := b.createAggSelect(AggregateFuncCOUNT, aggregate.Count)
	for k, v := range aggSelect2 {
		aggSelect[k] = v
	}

	aggSelect2 = b.createAggSelect(AggregateFuncSUM, aggregate.Sum)
	for k, v := range aggSelect2 {
		aggSelect[k] = v
	}

	aggSelect2 = b.createAggSelect(AggregateFuncAVG, aggregate.Avg)
	for k, v := range aggSelect2 {
		aggSelect[k] = v
	}

	aggSelect2 = b.createAggSelect(AggregateFuncMAX, aggregate.Max)
	for k, v := range aggSelect2 {
		aggSelect[k] = v
	}

	aggSelect2 = b.createAggSelect(AggregateFuncMIN, aggregate.Min)
	for k, v := range aggSelect2 {
		aggSelect[k] = v
	}

	if len(aggSelect) == 0 {
		return nil, errors.New("No aggregate fields found.")
	}

	aggSelect["_id"] = b.createGroupBySelect(aggregate.GroupBy)

	return aggSelect, nil
}

func (b *AggregateBuilder) createAggSelect(fn AggregateFunc, fields []string) bson.M {
	if fields == nil {
		return bson.M{}
	}

	agg := make(map[string]interface{})

	for _, field := range fields {
		aggAlias := fmt.Sprintf("%s_%s", fn, field)
		fieldAlias := fmt.Sprintf("$%s", getSchemaKey(field))
		if fn == "count" {
			agg[aggAlias] = bson.M{
				"$sum": bson.M{
					"$cond": bson.M{
						"if": bson.M{
							"$in": bson.A{
								bson.M{
									"$type": fieldAlias,
								},
								bson.A{"missing", "null"},
							},
						},
						"then": 0,
						"else": 1,
					},
				},
			}

			return agg
		}

		agg[aggAlias] = bson.M{fmt.Sprintf("$%s", fn): fieldAlias}
	}

	return agg
}

func (b *AggregateBuilder) createGroupBySelect(fields []string) bson.M {
	if fields == nil {
		return nil
	}

	m := bson.M{}

	for _, field := range fields {
		aggAlias := b.getGroupByAlias(field)
		fieldAlias := fmt.Sprintf("$%s", getSchemaKey(field))
		m[aggAlias] = fieldAlias
	}

	return m
}

func (b *AggregateBuilder) getGroupByAlias(field string) string {
	return fmt.Sprintf("group_by_%s", field)
}

func (b *AggregateBuilder) getGroupBySelects(fields []string) []string {
	if fields == nil {
		return nil
	}

	// append _id so it pulls the sort from the _id field
	var r = make([]string, len(fields))
	for i, field := range fields {
		r[i] = fmt.Sprintf("_id.%s", b.getGroupByAlias(field))
	}
	return r
}

func ConvertToAggregateResponse(aggregates []bson.M) ([]*types.AggregateResponse, error) {
	r := make([]*types.AggregateResponse, len(aggregates))
	for i, aggregate := range aggregates {
		ar := &types.AggregateResponse{}

		agg, err := extractResponse(aggregate["_id"].(bson.M))
		ar.Merge(agg)

		agg, err = extractResponse(aggregate)
		ar.Merge(agg)
		if err != nil {
			return nil, err
		}

		r[i] = ar
	}

	return r, nil
}

func extractResponse(response bson.M) (*types.AggregateResponse, error) {
	if response == nil {
		return &types.AggregateResponse{}, nil
	}

	agg := &types.AggregateResponse{}

	for resultField, _ /*v*/ := range response {
		if resultField == "_id" {
			continue
		}

		matchResult := AGG_REGEXP.FindAllStringSubmatch(resultField, -1)

		if len(matchResult[0]) != 3 {
			return nil, errors.New(fmt.Sprintf("Unknown aggregate column encountered for %s.", resultField))
		}

		matchedFunc := matchResult[0][1]
		matchedFieldName := matchResult[0][2]

		aggFunc := matchedFunc // camelCase(matchedFunc.toLowerCase())
		fieldName := matchedFieldName

		agg.Append(aggFunc, fieldName, response[resultField])
	}

	return agg, nil
}
