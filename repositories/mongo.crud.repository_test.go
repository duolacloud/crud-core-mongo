package repositories

import (
	"context"
	"testing"
	"time"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"duolacloud.com/duolacloud/crud-core/types"
)

type UserEntity struct {
	ID string `bson:"_id"`
	Name string `bson:"name"`
	Age int64 `bson:"age"`
	Birthday time.Time `bson:"birthday"`
}

var userSchema = bson.M{
	"$jsonSchema": bson.M{
		"bsonType": "object",
		"required": []string{"_id"},
		"properties": bson.M{
			"_id": bson.M{
				"bsonType":    "string",
				"description": "primary identifier",
			},
			"name": bson.M{
				"bsonType":    "string",
				"description": "name of user",
			},
			"age": bson.M{
				"bsonType":    "int",
				"description": "age of user",
			},
			"birthday": bson.M{
				"bsonType":    "date",
				"description": "birthday of user",
			},
		},
	},
}

func TestMongoCrudRepository(t *testing.T) {
	option := options.Client().ApplyURI("mongodb://localhost:27017")
	option.SetAuth(options.Credential{
		Username:"root",
		Password: "root",
	})

	client, err := mongo.Connect(context.TODO(), option)
	if err != nil {
		t.Fatal(err)
	}

	err = client.Ping(context.TODO(), nil)
	if err != nil {
		t.Fatal(err)
	}
	
	t.Log("connect mongo success")
	db := client.Database("test")

	s := NewMongoCrudRepository[UserEntity, UserEntity, UserEntity](db, "users", userSchema)

	err = s.Delete(context.TODO(), "1")
	if err != nil {
		t.Fatal(err)
	}

	birthday, _ := time.Parse("2006-01-02 15:04:05", "1989-03-02 12:00:01")
	t.Logf("birthday: %s\n", birthday)

	u, err := s.Create(context.TODO(), &UserEntity{
		ID: "1",
		Name: "张三",
		Age: 18,
		Birthday: birthday,
	})

	if err != nil {
		t.Fatal(err)
	}

	u, err = s.Update(context.TODO(), "1", &UserEntity{
		Name: "李四",
		Age: 19,
		Birthday: birthday,
	})
	if err != nil {
		t.Fatal(err)
	}

	u, err = s.Get(context.TODO(), "1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("修改后: %v", u)

	query := &types.PageQuery[UserEntity]{
		Filter: map[string]interface{}{
			"age": map[string]interface{}{
				"between": map[string]interface{}{
					"lower": 18,
					"upper": 20,
				},
			},
			"birthday": map[string]interface{}{
				"gt": "1987-02-02T12:00:01Z",
			},
		},
	}

	us, err := s.Query(context.TODO(), query)
	if err != nil {
		t.Fatal(err)
	}

	for _, i := range us {
		t.Logf("记录: %v", i)
	}

	count, err := s.Count(context.TODO(), query)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("记录总数: %v", count)
}
