package query

import(
	"fmt"
	"duolacloud.com/duolacloud/crud-core/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoQuery [Entity any] struct {
	FilterQuery bson.M
	Options *options.FindOptions
}


type FilterQueryBuilder[Entity any] struct {
	whereBuilder *WhereBuilder[Entity]
	strictValidation bool
	fieldTypes map[string]string
}

func NewFilterQueryBuilder[Entity any](
	schema bson.M,
	strictValidation bool,
) *FilterQueryBuilder[Entity] {
	b := &FilterQueryBuilder[Entity]{
		strictValidation: strictValidation,
	}

	if schema != nil {
		b.discoverFields(schema)
	}

	b.whereBuilder = NewWhereBuilder[Entity](b.fieldTypes)

	return b
}

func (b *FilterQueryBuilder[Entity]) discoverFields(schema bson.M) {
	if b.fieldTypes == nil {
		b.fieldTypes = map[string]string{}
	}

	// check top level is $jsonSchema
	if js, ok := schema["$jsonSchema"]; ok {
		schema = js.(bson.M)
	}

	// bsonType, required, properties at top level
	// looking for properties field, specifically
	if properties, ok := schema["properties"]; ok {
		properties := properties.(bson.M)
		b.iterateProperties("", properties)
	}
}

func (b *FilterQueryBuilder[Entity]) iterateProperties(parentPrefix string, properties bson.M) {
	// iterate each field within properties
	for field, value := range properties {
		switch value := value.(type) {
		case bson.M:
			// retrieve the type of the field
			if bsonType, ok := value["bsonType"]; ok {
				bsonType := bsonType.(string)
				// capture type in the fieldTypes map
				if bsonType != "" {
					b.fieldTypes[fmt.Sprintf("%s%s", parentPrefix, field)] = bsonType
				}

				if bsonType == "array" {
					// look at "items"
					if items, ok := value["items"]; ok {
						value = items.(bson.M)
					}
				}

				if subProperties, ok := value["properties"]; ok {
					subProperties := subProperties.(bson.M)
					b.iterateProperties(
						fmt.Sprintf("%s%s.", parentPrefix, field),
						subProperties,
					)
				}

				continue
			}

			// check for enum (without bsonType specified)
			if _, ok := value["enum"]; ok {
				b.fieldTypes[fmt.Sprintf("%s%s", parentPrefix, field)] = "object"
			}
		default:
			// unknown type
			continue
		}
	}
}

func (b *FilterQueryBuilder[Entity]) BuildQuery(query *types.PageQuery[Entity]) (*MongoQuery[Entity], error) {
	filterQuery, err := b.buildFilterQuery(query.Filter)
	if err != nil {
		return nil, err
	}

	sort, err := b.buildSorting(query.Sort)
	if err != nil {
		return nil, err
	}

	opts := &options.FindOptions{
		Skip: &query.Offset,
		Limit: &query.Limit,
		Sort: sort,
	}

	fmt.Printf("filterQuery: %v\n", filterQuery)

	return &MongoQuery[Entity]{
		FilterQuery: filterQuery,
		Options: opts,
	}, nil
}

func (b *FilterQueryBuilder[Entity]) buildFilterQuery(filter map[string]interface{}) (bson.M, error) {
	if filter == nil {
		return bson.M{}, nil
	}

	return b.whereBuilder.build(filter)
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
				if _, ok := b.fieldTypes[field]; !ok {
					// we have a problem
					return nil, fmt.Errorf("field %s does not exist in collection", field)
				}
			}

			sort[field] = val
		}
	}

	return sort, nil
}