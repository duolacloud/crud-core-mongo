package query

import(
	"fmt"
	"time"
	"errors"
	"github.com/duolacloud/crud-core/types"
	mongo_schema "github.com/duolacloud/crud-core-mongo/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoQuery struct {
	FilterQuery bson.M
	Options *options.FindOptions
}

type MongoAggregateQuery struct {
	MongoQuery
	Aggregate bson.M
}

type MongoCursorQuery struct {
	MongoQuery
	Reverse bool
}

type FilterQueryBuilder[Entity any] struct {
	whereBuilder *WhereBuilder[Entity]
	aggregateBuilder *AggregateBuilder
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
	b.aggregateBuilder = NewAggregateBuilder()

	return b
}

func (b *FilterQueryBuilder[Entity]) BuildQuery(query *types.PageQuery) (*MongoQuery, error) {
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

	return &MongoQuery{
		FilterQuery: filterQuery,
		Options: opts,
	}, nil
}

func (b *FilterQueryBuilder[Entity]) BuildAggregateQuery(aggregate *types.AggregateQuery, filter map[string]any) (*MongoAggregateQuery, error) {
	filterQuery, err := b.buildFilterQuery(filter)
	if err != nil {
		return nil, err
	}

	aggr, err := b.aggregateBuilder.build(aggregate)
	if err != nil {
		return nil, err
	}

	opts := &options.FindOptions{}

	sort, err := b.buildAggregateSorting(aggregate)
	if err != nil {
		return nil, err
	}

	if sort != nil {
		opts.Sort = sort
	}


	return &MongoAggregateQuery{
		MongoQuery: MongoQuery{
			FilterQuery: filterQuery,
			Options: opts,
		},
		Aggregate: aggr,
	}, nil
}

func (b *FilterQueryBuilder[Entity]) buildAggregateSorting(aggregate *types.AggregateQuery) (map[string]int, error) {
	aggregateGroupBy := b.aggregateBuilder.getGroupBySelects(aggregate.GroupBy)
	if aggregateGroupBy == nil {
		return nil, nil
	}

	var sort = make(map[string]int)

	for _, field := range aggregateGroupBy {
		sort[field] = 1
	}

	return sort, nil
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
			opts.SetSkip(int64((page-1) * size))
		}
	}
}

func (b *FilterQueryBuilder[Entity]) buildFilterQuery(filter map[string]any) (bson.M, error) {
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

			// TODO 这里打个补丁先，正常要读取 结构体的 元数据 来得到 字段名
			if field == "id" {
				field = "_id"
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

			// TODO 这里打个补丁先，正常要读取 结构体的 元数据 来得到 字段名
			if field == "id" {
				field = "_id"
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









func (b *FilterQueryBuilder[Entity]) BuildCursorQuery(query *types.CursorQuery) (*MongoCursorQuery, error) {
	filterQuery, err := b.buildFilterQuery(query.Filter)
	if err != nil {
		return nil, err
	}

	b.ensureOrders(query)

	cursorFilter, err := b.buildCursorFilter(query)
	if err != nil {
		return nil, err
	}

	filters := bson.M{"$and": []bson.M{cursorFilter, filterQuery}}


	sort, err := b.buildSorting(query.Sort)
	if err != nil {
		return nil, err
	}

	limit := query.Limit + 1

	opts := &options.FindOptions{
		Sort: sort,
		Limit: &limit,
	}

	prj, err := b.buildProjections(query.Fields)
	if err != nil {
		return nil, err
	}
	if len(prj) > 0 {
		opts.SetProjection(prj)
	}

	return &MongoCursorQuery{
		MongoQuery: MongoQuery{
			FilterQuery: filters,
			Options: opts,
		},
	}, nil
}

func (b *FilterQueryBuilder[Entity]) ensureOrders(query *types.CursorQuery) {
	hasId := false
	for _, sortField := range query.Sort {
		if sortField[0:1] == "-" {
			sortField = sortField[1:]
		}

		if sortField[0:1] == "+" {
			sortField = sortField[1:]
		}

		if sortField == "id" {
			hasId = true
		}
	}

	// 没有id的排序，直接要追加 ID
	if !hasId {
		index := 0
		if query.Sort == nil {
			query.Sort = make([]string, 1)
			query.Sort[0] = "id"
		} else {
			tmp := query.Sort
			query.Sort = make([]string, len(query.Sort) + 1)
			query.Sort[0] = "id"
			for i, f := range tmp {
				query.Sort[i+1] = f
				index++
			}
		}
	}
}

func (b *FilterQueryBuilder[Entity]) buildCursorFilter(query *types.CursorQuery) (bson.M, error) {
	ors := []bson.M{}

	if len(query.Cursor) > 0 {
		cursor := &types.Cursor{}
		err := cursor.Unmarshal(query.Cursor)
		if err != nil {
			return nil, err
		}

		if len(cursor.Value) == 0 {
			return nil, nil
		}

		if len(cursor.Value) != len(query.Sort) {
			return nil, errors.New(fmt.Sprintf("cursor format fields length: %d not match orders fields length: %d", len(cursor.Value), len(query.Sort)))
		}

		fields := make([]string, len(cursor.Value))
		values := make([]any, len(cursor.Value))

		for i, value := range cursor.Value {
			// val := 1
			sortField := query.Sort[i]

			if sortField[0:1] == "-" {
				sortField = sortField[1:]
				// val = -1
			}

			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			// TODO 这里打个补丁先，正常要读取 结构体的 元数据 来得到 字段名
			if sortField == "id" {
				sortField = "_id"
			}
	
			sort_field_type, ok := b.schema.FieldTypes[sortField]
			if !ok {
				err := errors.New(fmt.Sprintf("ERR_DB_UNKNOWN_FIELD %s", sortField))
				return nil, err
			}
			fields[i] = sortField

			switch sort_field_type {
			case "date", "timestamp":
				// 本身就是 time 类型
				if t, ok := value.(time.Time); ok {
					values[i] = t
				}

				v, err := time.Parse(time.RFC3339, value.(string))
				if err == nil {
					values[i] = v
				}
			default:
				values[i] = value
			}
		}


		sort_field_0_direction := 1
		sort_field_0 := query.Sort[0]

		if sort_field_0[0:1] == "-" {
			sort_field_0 = sort_field_0[1:]
			sort_field_0_direction = -1
		}

		if sort_field_0[0:1] == "+" {
			sort_field_0 = sort_field_0[1:]
		}

		var cmp string

		if query.Direction == types.CursorDirectionBefore {
			// before
			if sort_field_0_direction == -1 {
				cmp = "$gt"
			} else {
				cmp = "$lt"
			}
		} else {
			// after
			if sort_field_0_direction == -1 {
				cmp = "$lt"
			} else {
				cmp = "$gt"
			}
		}

		var ands []bson.M
		for i, field := range fields {
			ands = append(ands, bson.M{ field: bson.M{ cmp: values[i] }})

			ors = append(ors, bson.M{"$and": ands})
		}
	}
	
	cursorFilter := bson.M{}
	if len(ors) > 0 {
		cursorFilter["$or"] = ors
	}

	fmt.Printf("cursorFilter: %v\n", cursorFilter)

	return cursorFilter, nil
}	