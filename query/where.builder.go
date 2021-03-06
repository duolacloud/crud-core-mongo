package query

import(
	"go.mongodb.org/mongo-driver/bson"
	"github.com/duolacloud/crud-core/types"
	mongo_schema "github.com/duolacloud/crud-core-mongo/schema"
)

type WhereBuilder [Entity any] struct {
	comparisonBuilder *ComparisonBuilder[Entity]
}

func NewWhereBuilder[Entity any](schema *mongo_schema.Schema) *WhereBuilder[Entity] {
	return &WhereBuilder[Entity]{
		comparisonBuilder: NewComparisonBuilder[Entity](DEFAULT_COMPARISON_MAP, schema),
	}
}

func(b *WhereBuilder[Entity]) build(filter map[string]any) (bson.M, error) {
	var ands []bson.M
	var ors []bson.M
	filterQuery := bson.M{}

	
	if and, ok := filter["and"]; ok {
		if andArr, ok := and.([]map[string]any); ok {
			for _, f := range andArr {
				o, err := b.build(f)
				if err != nil {
					return nil, err
				}
				ands = append(ands, o)
			}
		}
	}

	if or, ok := filter["or"]; ok {
		if orMap, ok := or.([]map[string]any); ok {
			for _, f := range orMap {
				o, err := b.build(f)
				if err != nil {
					return nil, err
				}
				ors = append(ors, o)
			}
		}	
	}

	filterAnds, err := b.filterFields(filter)
	if err != nil {
		return nil, err
	}

	if filterAnds != nil {
		ands = append(ands, filterAnds)
	}

	if len(ands) > 0 {
		filterQuery["$and"] = ands
	}

	if len(ors) > 0 {
		filterQuery["$or"] = ors
	}

	return filterQuery, nil
}

func(b *WhereBuilder[Entity]) filterFields(filter map[string]any) (bson.M, error) {
	var ands []bson.M
	for field, cmp := range filter {
		if field == "and" || field == "or" {
			continue
		}

		and, err := b.withFilterComparison(field, cmp.(map[string]any))
		if err != nil {
			return nil, err
		}

		ands = append(ands, and)
	}

	if len(ands) == 1 {
		return ands[0], nil
	}

	if len(ands) > 0 {
		return bson.M{ "$and": ands }, nil
	}

	return nil, nil
}

func(b *WhereBuilder[Entity]) withFilterComparison(
	field string,
	cmp map[string]any,
) (bson.M, error) {
	var opts []types.FilterComparisonOperators
	for key, _ := range cmp {
		opts = append(opts, types.FilterComparisonOperators(key))
	}

	if len(opts) == 1 {
		cmpType := opts[0]
		return b.comparisonBuilder.build(field, cmpType, cmp[string(cmpType)])
	}

	var ors []bson.M

	for _, cmpType := range opts {
		m, err := b.comparisonBuilder.build(field, cmpType, cmp[string(cmpType)])
		if err != nil {
			return nil, err
		}

		ors = append(ors, m)
	}

	return bson.M{
		"$or": ors,
	}, nil
}