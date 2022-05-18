package repositories

import (
	"context"
	"duolacloud.com/duolacloud/crud-core/types"
	"duolacloud.com/duolacloud/crud-core-mongo/query"
	mongo_schema "duolacloud.com/duolacloud/crud-core-mongo/schema"
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

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery) (int64, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.Schema, r.Options.StrictValidation)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return 0, err
	}

	count, err := r.DB.Collection(r.Collection).CountDocuments(c, mq.FilterQuery)
	return count, err
}