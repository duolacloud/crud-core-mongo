package repositories

import (
	"context"
	"testing"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"duolacloud.com/duolacloud/crud-core/types"
)

type UserEntity struct {
	ID string `bson:"_id"`
	Name string `bson:"name"`
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

	s := NewMongoCrudRepository[UserEntity, UserEntity, UserEntity](db, "users")

	err = s.Delete(context.TODO(), "1")
	if err != nil {
		t.Fatal(err)
	}

	u, err := s.Create(context.TODO(), &UserEntity{
		ID: "1",
		Name: "张三",
	})

	if err != nil {
		t.Fatal(err)
	}

	u, err = s.Update(context.TODO(), "1", &UserEntity{
		Name: "李四",
	})
	if err != nil {
		t.Fatal(err)
	}

	u, err = s.Get(context.TODO(), "1")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("修改后: %v", u)

	count, err := s.Count(context.TODO(), &types.PageQuery[UserEntity]{})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("记录总数: %v", count)
}
