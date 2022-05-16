package repositories

import (
	"context"
	"duolacloud.com/duolacloud/crud-core/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	db *mongo.Database
	collection string
}

func NewMongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	db *mongo.Database,
	collection string,
) *MongoCrudRepository[DTO, CreateDTO, UpdateDTO] {
	return &MongoCrudRepository[DTO, CreateDTO, UpdateDTO]{
		db: db,
		collection: collection,
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

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, query *types.PageQuery[DTO]) ([]*DTO, error) {
	skip := (query.Page-1) * int64(query.Limit)

	opts := options.FindOptions{
		Skip: &skip,
		Limit: &query.Limit,
	}

	// TODO 通过 query 转换
	filter := bson.M{}

	// TODO 转换 sort
	opts.SetSort(query.Sort)

	cursor, err := r.db.Collection(r.collection).Find(c, filter, &opts)
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

func (r *MongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Count(c context.Context, query *types.PageQuery[DTO]) (int64, error) {
	filter := bson.D{{}}

	count, err := r.db.Collection(r.collection).CountDocuments(c, filter)
	return count, err
}