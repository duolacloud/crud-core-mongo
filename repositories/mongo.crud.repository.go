package repositories

import (
	"context"
	"duolacloud.com/duolacloud/crud-core/types"
	"duolacloud.com/duolacloud/crud-core-mongo/query"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	db *mongo.Database
	collection string
	schema bson.M
}

func NewMongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	db *mongo.Database,
	collection string,
	schema bson.M,
) *MongoCrudRepository[DTO, CreateDTO, UpdateDTO] {
	return &MongoCrudRepository[DTO, CreateDTO, UpdateDTO]{
		db: db,
		collection: collection,
		schema: schema,
	}
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO, opts ...types.CreateOption) (*DTO, error) {
	_, err := r.db.Collection(r.collection).InsertOne(c, createDTO)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	_, err := r.db.Collection(r.collection).DeleteOne(c, bson.M{"_id": id})
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

	err = r.db.Collection(r.collection).FindOneAndUpdate(c, filter, update, &mongo_opts).Decode(&dto)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto *DTO

	filter := bson.D{{"_id", id}}

	err := r.db.Collection(r.collection).FindOne(c, filter).Decode(&dto)
	if err != nil {
		return nil, err
	}

	return dto, nil
}

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, q *types.PageQuery[DTO]) ([]*DTO, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.schema, true)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return nil, err
	}

	cursor, err := r.db.Collection(r.collection).Find(c, mq.FilterQuery, mq.Options)
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

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, q *types.PageQuery[DTO]) (int64, error) {
	filterQueryBuilder := query.NewFilterQueryBuilder[DTO](r.schema, true)

	mq, err := filterQueryBuilder.BuildQuery(q);
	if err != nil {
		return 0, err
	}

	count, err := r.db.Collection(r.collection).CountDocuments(c, mq.FilterQuery)
	return count, err
}