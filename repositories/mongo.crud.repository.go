package repositories

import (
	"context"
	"duolacloud.com/duolacloud/crud-core/repositories"
	"duolacloud.com/duolacloud/crud-core/types"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type mongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any] struct {
	db *mongo.Database
	collection string
}

func NewMongoCrudRepository[DTO any, CreateDTO any, UpdateDTO any](
	db *mongo.Database,
	collection string,
) repositories.CrudRepository[DTO, CreateDTO, UpdateDTO] {
	return &mongoCrudRepository[DTO, CreateDTO, UpdateDTO]{
		db: db,
		collection: collection,
	}
}

func (r *mongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Create(c context.Context, createDTO *CreateDTO) (*DTO, error) {
	_, err := r.db.Collection(r.collection).InsertOne(c, createDTO)
	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (r *mongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Delete(c context.Context, id types.ID) error {
	_, err := r.db.Collection(r.collection).DeleteOne(c, bson.M{"_id": id})
	return err
}

func (r *mongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Update(c context.Context, id types.ID, updateDTO *UpdateDTO, opts ...types.UpdateOption) (*DTO, error) {
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

func (r *mongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Get(c context.Context, id types.ID) (*DTO, error) {
	var dto *DTO

	filter := bson.D{{"_id", id}}

	err := r.db.Collection(r.collection).FindOne(c, filter).Decode(&dto)
	if err != nil {
		return nil, err
	}

	return dto, nil
}

func (r *mongoCrudRepository[DTO, CreateDTO, UpdateDTO]) Query(c context.Context, query *types.PageQuery[DTO]) ([]*DTO, error) {
	return nil, nil
}