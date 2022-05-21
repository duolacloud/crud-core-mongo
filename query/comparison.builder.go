package query

import(
	"time"
	"strings"
	"errors"
	"fmt"
	"strconv"
	"github.com/duolacloud/crud-core/types"
	mongo_schema "github.com/duolacloud/crud-core-mongo/schema"
	"github.com/duolacloud/crud-core-mongo/utils"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var DEFAULT_COMPARISON_MAP = map[string]string{
	"eq": "$eq",
	"neq": "$ne",
	"gt": "$gt",
	"gte": "$gte",
	"lt": "$lt",
	"lte": "$lte",
	"in": "$in",
	"notin": "$nin",
	"is": "$eq",
	"isnot": "$ne",
}


type ComparisonBuilder [Entity any] struct {
	comparisonMap map[string]string
	schema *mongo_schema.Schema
}

func NewComparisonBuilder[Entity any](
	comparisonMap map[string]string,
	schema *mongo_schema.Schema,
) *ComparisonBuilder[Entity] {
	var _comparisonMap map[string]string
	if comparisonMap != nil {
		_comparisonMap = comparisonMap
	} else {
		_comparisonMap = DEFAULT_COMPARISON_MAP
	}

	return &ComparisonBuilder[Entity]{
		comparisonMap: _comparisonMap,
		schema: schema,
	}
}


func (b *ComparisonBuilder[Entity]) build(
	field string,
	cmp types.FilterComparisonOperators,
	val interface{},
) (bson.M, error) {
	schemaKey := field // TODO 根据字段名 获取 数据库的字段名

	normalizedCmp := strings.ToLower(string(cmp));
	var querySelector bson.M

	// TODO 根据 cmp 判断 val 类型
	switch normalizedCmp {
	case "in":
		if !utils.IsArray(val) {
			return nil, errors.New("Invalid value, value for in must be array")
		}
	}

	if cmp, ok := b.comparisonMap[normalizedCmp]; ok {
		// comparison operator (e.b. =, !=, >, <)
		_cmp, err := b.convertQueryValue(field, val)
		if err != nil {
			return nil, err
		}

		querySelector = bson.M{
			cmp: _cmp,
		}
	}

	/* TODO
	if (strings.Contains(normalizedCmp, "like")) {
		querySelector = b.likeComparison(normalizedCmp, val);
	}*/

	if (strings.Contains(normalizedCmp, "between")) {
		var err error
		querySelector, err = b.betweenComparison(normalizedCmp, field, val)
		if err != nil {
			return nil, err
		}
	}

	if querySelector == nil {
		return nil, errors.New(fmt.Sprintf("unknown operator (%v)", cmp))
	}

	return bson.M{ schemaKey: querySelector }, nil
}

func (b *ComparisonBuilder[Entity]) betweenComparison(
	cmp string,
	field string,
	val interface{},
) (bson.M, error) {
	if !b.isBetweenVal(val) {
		return nil, errors.New(fmt.Sprintf("Invalid value for %v expected {lower: val, upper: val} got %v", val));
	}

	_val, ok := val.(map[string]interface{})
	if !ok {
		return nil, errors.New("Invalid value, value must be a map");	
	}

	if cmp == "notbetween" {
		if !ok {
			return nil, errors.New("val not type of map[string]interface{}")
		}

		lt, err := b.convertQueryValue(field, _val["lower"])
		if err != nil {
			return nil, err
		}

		gt, err := b.convertQueryValue(field, _val["upper"])
		if err != nil {
			return nil, err
		}

		return bson.M{ 
			"$lt": lt, 
			"$gt": gt,
		}, nil
	}
	

	gte, err := b.convertQueryValue(field, _val["lower"])
	if err != nil {
		return nil, err
	}

	lte, err := b.convertQueryValue(field, _val["upper"])
	if err != nil {
		return nil, err
	}

	return bson.M{
		"$gte": gte,
		"$lte": lte,
	}, nil
}

func (b *ComparisonBuilder[Entity]) isBetweenVal(
	val interface{},
) bool {
	if val == nil {
		return false
	}

	m, ok := val.(map[string]interface{})
	if !ok {
		return false
	}

	if m["lower"] == nil {
		return false
	}

	if m["upper"] == nil {
		return false
	}

	return true
}

/*
func (b *ComparisonBuilder) likeComparison(
	cmp string,
	val interface{},
) *bson.M {
	regExpStr := escapeRegExp(val).replace(/%/g, '.*');
	regExp := RegExp(regExpStr, cmp.includes("ilike") ? "i" : nil);
	
	if cmp.startsWith("not") {
		return &bson.M{ "$not": bson.M{ "$regex": regExp } };
	}
	return &bson.M{ "$regex": regExp };
}
*/

func (b *ComparisonBuilder[Entity]) convertQueryValue(field string, val interface{}) (any, error) {
	if field == "_id" || field == "id"  {
		return b.convertToObjectId(val)
	}

	bsonType, ok := b.schema.FieldTypes[field]
	if ok {
		switch bsonType {
		case "string":
			return val, nil
		case "bool":
			// 先看是不是默认就是 bool
			bv, ok := val.(bool)
			if !ok {
				bv, _ = strconv.ParseBool(val.(string))
			}
			return bv, nil
		case "date", "timestamp":
			// 已经是 time类型，直接返回
			dv, ok := val.(time.Time)
			if ok {
				return dv, nil
			}

			dv, _ = time.Parse(time.RFC3339, val.(string))
			return dv, nil
		case "decimal", "double", "int", "long":
			// 如果是字符串，就需要做一下转换
			if sv, ok := val.(string); ok {
				var bitSize int
				switch bsonType {
				case "decimal":
					bitSize = 32
				case "double":
					bitSize = 64
				case "int":
					bitSize = 32
				case "long":
					bitSize = 64
				}

				var pv interface{}
				if bsonType == "decimal" || bsonType == "double" {
					v, _ := strconv.ParseFloat(sv, bitSize)
					pv = v
					// retype 32 bit
					if bitSize == 32 {
						pv = float32(v)
					}
				} else {
					v, _ := strconv.ParseInt(sv, 0, bitSize)
					pv = v
					// retype 32 bit
					if bitSize == 32 {
						pv = int32(v)
					}
				}
				return pv, nil
			}
			return val, nil
		case "object":
			return val, nil
		}
	}

	return val, nil
}

func (b *ComparisonBuilder[Entity]) convertToObjectId(val interface{}) (any, error) {
	if objIDs, ok := val.([]string); ok {
		var r []interface{}
		for _, v := range objIDs {
			id, err := b.convertToObjectId(v)
			if err != nil {
				return nil, err
			}
			r = append(r, id)
		}
		return r, nil
	}

	switch t := val.(type) {
	case string:
		if primitive.IsValidObjectID(t) {
			return primitive.ObjectIDFromHex(t)
		}
	}

	return val, nil
}