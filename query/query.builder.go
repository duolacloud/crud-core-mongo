package query

import(
	"fmt"
	"duolacloud.com/duolacloud/crud-core/types"
	mongo_schema "duolacloud.com/duolacloud/crud-core-mongo/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoQuery [Entity any] struct {
	FilterQuery bson.M
	Options *options.FindOptions
}


type FilterQueryBuilder[Entity any] struct {
	whereBuilder *WhereBuilder[Entity]
	schema *mongo_schema.Schema
	strictValidation bool
}

func NewFilterQueryBuilder[Entity any](
	schema *mongo_schema.Schema,
	strictValidation bool,
) *FilterQueryBuilder[Entity] {
	b := &FilterQueryBuilder[Entity]{
		strictValidation: strictValidation,
		schema: schema,
	}

	b.whereBuilder = NewWhereBuilder[Entity](schema)

	return b
}

func (b *FilterQueryBuilder[Entity]) BuildQuery(query *types.PageQuery) (*MongoQuery[Entity], error) {
	filterQuery, err := b.buildFilterQuery(query.Filter)
	if err != nil {
		return nil, err
	}

	sort, err := b.buildSorting(query.Sort)
	if err != nil {
		return nil, err
	}

	opts := &options.FindOptions{
		Sort: sort,
	}
	b.setPaginationOptions(query.Page, opts)

	prj, err := b.buildProjections(query.Fields)
	if err != nil {
		return nil, err
	}
	if len(prj) > 0 {
		opts.SetProjection(prj)
	}

	return &MongoQuery[Entity]{
		FilterQuery: filterQuery,
		Options: opts,
	}, nil
}

func (b *FilterQueryBuilder[Entity]) setPaginationOptions(pagination map[string]int, opts *options.FindOptions) {
	// check for limit
	if limit, ok := pagination["limit"]; ok {
		opts.SetLimit(int64(limit))

		// check for offset (once limit is set)
		if offset, ok := pagination["offset"]; ok {
			opts.SetSkip(int64(offset))
		}

		// check for skip (once limit is set)
		if skip, ok := pagination["skip"]; ok {
			opts.SetSkip(int64(skip))
		}
	}

	// check for page and size
	if size, ok := pagination["size"]; ok {
		opts.SetLimit(int64(size))

		// set skip (requires understanding of size)
		if page, ok := pagination["page"]; ok {
			opts.SetSkip(int64(page * size))
		}
	}
}

func (b *FilterQueryBuilder[Entity]) buildFilterQuery(filter map[string]interface{}) (bson.M, error) {
	if filter == nil {
		return bson.M{}, nil
	}

	return b.whereBuilder.build(filter)
}

func (b *FilterQueryBuilder[Entity]) buildProjections(fields []string) (map[string]int, error) {
	prj := map[string]int{}
	// set field projections option
	if len(fields) > 0 {
		for _, field := range fields {
			val := 1

			// handle when the first char is a - (don't display field in result)
			if field[0:1] == "-" {
				field = field[1:]
				val = 0
			}

			// handle scenarios where the first char is a + (redundant)
			if field[0:1] == "+" {
				field = field[1:]
			}

			// lookup field in the fieldTypes dictionary if strictValidation is true
			if b.strictValidation {
				if _, ok := b.schema.FieldTypes[field]; !ok {
					// we have a problem
					return nil, fmt.Errorf("field %s does not exist in collection", field)
				}
			}

			// add the field to the project dictionary
			prj[field] = val
		}
	}

	return prj, nil
}

func (b *FilterQueryBuilder[Entity]) buildSorting(fields []string) (map[string]int, error) {
	sort := map[string]int{}
	if len(fields) > 0 {
		for _, field := range fields {
			val := 1

			if field[0:1] == "-" {
				field = field[1:]
				val = -1
			}

			if field[0:1] == "+" {
				field = field[1:]
			}

			// lookup field in the fieldTypes dictionary if strictValidation is true
			if b.strictValidation {
				if _, ok := b.schema.FieldTypes[field]; !ok {
					// we have a problem
					return nil, fmt.Errorf("field %s does not exist in collection", field)
				}
			}

			sort[field] = val
		}
	}

	return sort, nil
}