package repositories

import (
	"fmt"
	"bytes"
	// "errors"
	"context"
	"github.com/duolacloud/crud-core/types"
	"github.com/duolacloud/crud-core-mongo/query"
	mongo_schema "github.com/duolacloud/crud-core-mongo/schema"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCrudRepositoryOptions struct {
	StrictValidation bool
}

type MongoCrudRepositoryOption func (*MongoCrudRepositoryOptions)

func WithStrictValidation(v bool) MongoCrudRepositoryOption {
	return func(o *MongoCrudRepositoryOptions) {
		o.StrictValidation = v
	}
}

type MongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	DB *mongo.Database
	Collection string
	Schema *mongo_schema.Schema
	Options *MongoCrudRepositoryOptions
}

func NewMongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	db *mongo.Database,
	collection string,
	schema bson.M,
	opts ...MongoCrudRepositoryOption,

) *MongoCrudRepository[DTO, CreateDTO, UpdateDTO] {
	r := &MongoCrudRepository[DTO, CreateDTO, UpdateDTO]{
		DB: db,
		Collection: collection,
		Schema: mongo_schema.NewSchema(schema),
	}

	r.Options = &MongoCrudRepositoryOptions{}
	for _, o := range opts {
		o(r.Options)
	}

	return r
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	_, err := r.DB.Collection(r.Collection).InsertOne(c, createDTO)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	_, err := r.DB.Collection(r.Collection).DeleteOne(c, bson.M{"_id": id})
	return err
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
	var dto *DTO

	var _opts types.UpdateOptions
	for _, o := range opts {
		o(&_opts)
	}

	mongo_opts := options.FindOneAndUpdateOptions{
		Upsert: &_opts.Upsert,
	}

	data, err := bson.Marshal(updateDTO)
	if err != nil {
		return nil, err
	}
	mmap := bson.M{}
	err = bson.Unmarshal(data, mmap)
	if err != nil {
		return nil, err
	}
	delete(mmap, "_id")

	filter := bson.D{{"_id", id}}
	update := bson.D{{"$set", mmap}}

	err = r.DB.Collection(r.Collection).FindOneAndUpdate(c, filter, update, &mongo_opts).Decode(&dto)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto *DTO

	filter := bson.D{{"_id", id}}

	err := r.DB.Collection(r.Collection).FindOne(c, filter).Decode(&dto)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, types.ErrNotFound
		}

		return nil, err
	}

	return dto, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, q *types.PageQuery) ([]*DTO, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return nil, err
	}

	cursor, err := r.DB.Collection(r.Collection).Find(c, mq.FilterQuery, mq.Options)
	if err != nil {
		return nil, err
	}

	var dtos []*DTO

	err = cursor.All(c, &dtos)
	if err != nil {
		return nil, err
	}

	return dtos, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) QueryOne(c context.Context, filter map[string]interface{}) (*DTO, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(&types.PageQuery{
		Filter: filter,
	});
	if err != nil {
		return nil, err
	}

	var dto *DTO
	err = r.DB.Collection(r.Collection).FindOne(c, mq.FilterQuery).Decode(&dto)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, types.ErrNotFound
		}

		return nil, err
	}

	return dto, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery) (int64, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return 0, err
	}

	count, err := r.DB.Collection(r.Collection).CountDocuments(c, mq.FilterQuery)
	return count, err
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Aggregate(
	c context.Context,
	filter map[string]interface{},
	aggregateQuery *types.AggregateQuery,
) ([]*types.AggregateResponse, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildAggregateQuery(aggregateQuery, filter)
	if err != nil {
		return nil, err
	}
	
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: mq.FilterQuery}},
		{{Key: "$group", Value: mq.Aggregate}},
	}

	fmt.Printf("xaggregate: %v\n", mq.Aggregate)

	if mq.MongoQuery.Options.Sort != nil {
		pipeline = append(pipeline, bson.D{{Key: "$sort", Value: mq.MongoQuery.Options.Sort}})
	}

	cursor, err := r.DB.Collection(r.Collection).Aggregate(c, pipeline)
	if err != nil {
		return nil, err
	}
	
	var result []bson.M
	err = cursor.All(c, &result)
	if err != nil {
		return nil, err
	}

	fmt.Printf("r: %v\n", result)

	return query.ConvertToAggregateResponse(result)
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) CursorQuery(c context.Context, q *types.CursorQuery) ([]*DTO, *types.CursorExtra, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildCursorQuery(q)
	if err != nil {
		return nil, nil, err
	}

	cursor, err := r.DB.Collection(r.Collection).Find(c, mq.FilterQuery, mq.Options)
	if err != nil {
		return nil, nil, err
	}

	var result []*DTO

	err = cursor.All(c, &result)
	if err != nil {
		return nil, nil, err
	}

	extra := &types.CursorExtra{}

	if len(result) == 0 {
		return nil, extra, nil
	}

	if len(result) == int(q.Limit + 1) {
		extra.HasNext = true
		extra.HasPrevious = true

		result = result[0:len(result)-1]
		fmt.Printf("len(result) == q.Limit itemCount: %d, limit: %d\n", len(result), q.Limit)
	}
	fmt.Printf("fuck itemCount: %d\n", len(result))

	toCursor := func(item *DTO) (string, error) {
		/*
		// 反射DTO类型
		t := reflect.TypeOf(item)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}*/

		
		sortFieldValues := make([]interface{}, len(q.Sort))
		for i, sortField := range q.Sort {
			if sortField[0:1] == "-" {
				sortField = sortField[1:]
			}
	
			if sortField[0:1] == "+" {
				sortField = sortField[1:]
			}

			if sortField == "id" {
				sortField = "_id"
			}

			/*
			_, ok := r.Schema.FieldTypes[sortField]
			if !ok {
				return "", errors.New(fmt.Sprintf("field %s not found", sortField))
			}
			*/
			var m bson.M
			bytes, _ := bson.Marshal(item)
			_ = bson.Unmarshal(bytes, &m)

			sortFieldValues[i] = m[sortField]
		}

		cursor := &types.Cursor{
			Value: sortFieldValues,
		}

		w := new(bytes.Buffer)
		err = cursor.Marshal(w)
		if err != nil {
			return "", err
		}

		return w.String(), nil
	}

	itemCount := len(result)
	extra.StartCursor, err = toCursor(result[0])
	if err != nil {
		return nil, nil, err
	}

	extra.EndCursor, err = toCursor(result[itemCount-1])
	if err != nil {
		return nil, nil, err
	}

	return result, extra, nil
}