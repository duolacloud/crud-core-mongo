package repositories

import (
	"fmt"
	"encoding/json"
	"context"
	"testing"
	"time"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/bson"
	"github.com/duolacloud/crud-core/types"
)

type UserEntity struct {
	ID string `bson:"_id"`
	Name string `bson:"name"`
	Country string `bson:"country"`
	Age int64 `bson:"age"`
	Birthday time.Time `bson:"birthday"`
}

func (u *UserEntity) BeforeCreate() {
	fmt.Printf("UserEntity.BeforeCreate")
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
			"country": bson.M{
				"bsonType":    "string",
				"description": "country of user",
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

	s := NewMongoCrudRepository[UserEntity, UserEntity, UserEntity](db, "users", userSchema, WithStrictValidation(true))

	err = s.Delete(context.TODO(), "1")
	if err != nil {
		t.Fatal(err)
	}

	birthday, _ := time.Parse("2006-01-02 15:04:05", "1989-03-02 12:00:01")
	t.Logf("birthday: %s\n", birthday)

	u, err := s.Create(context.TODO(), &UserEntity{
		ID: "1",
		Name: "张三",
		Country: "china",
		Age: 18,
		Birthday: birthday,
	})

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("create: %v\n", u)

	u, err = s.Update(context.TODO(), "1", &UserEntity{
		Name: "李四",
		Age: 19,
		Country: "china",
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

	query := &types.PageQuery{
		Fields: []string{
			"name",
			"_id",
		},
		Filter: map[string]any{
			"age": map[string]any{
				"between": map[string]any{
					"lower": 18,
					"upper": 24,
				},
			},
			/*"name": map[string]any{
				"in": []any{
					"李四",
					"哈哈",
				},
			},*/ 
			"birthday": map[string]any{
				"gt": "1987-02-02T12:00:01Z",
			},
		},
		Page: map[string]int{
			"limit": 1,
			"offset": 0,
		},
	}

	{
		u, err := s.QueryOne(context.TODO(), map[string]any{
			"name": map[string]any{
				"eq": "李四1",
			},
		})
		if err != nil && err != types.ErrNotFound {
			t.Fatal(err)
		}
		
		t.Logf("queryOne: %v\n", u)
	}
	
	us, err := s.Query(context.TODO(), query)
	if err != nil {
		t.Fatal(err)
	}
	
	for _, i := range us {
		t.Logf("记录: %v\n", i)
	}


	count, err := s.Count(context.TODO(), query)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("记录总数: %v\n", count)

	c, extra, err := s.CursorQuery(context.TODO(), &types.CursorQuery{
		// Cursor: "gaF2kaEx",
		Limit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	{
		js, _ := json.Marshal(c)
		extraJs, _ := json.Marshal(extra)
		t.Logf("CursorList: %v, %v\n", string(js), string(extraJs))
	}
	
	aggs, err := s.Aggregate(context.TODO(), query.Filter, &types.AggregateQuery{
		GroupBy: []string{
			"country",
		},
		Count: []string{
			"country",
		},
		Max: []string{
			"age",
		},
		Min: []string{
			"age",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, agg := range aggs {
		js, err := json.Marshal(agg)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("聚合: %v\n", string(js))
	}
}
