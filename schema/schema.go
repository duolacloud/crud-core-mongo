package schema

import (
	"fmt"

	"go.mongodb.org/mongo-driver/bson"
)

type Schema struct {
	FieldTypes map[string]string
}

func NewSchema(s bson.M) *Schema {
	schema := &Schema{}

	if s != nil {
		schema.discoverFields(s)
	}

	return schema
}

func (s *Schema) discoverFields(schema bson.M) {
	if s.FieldTypes == nil {
		s.FieldTypes = map[string]string{}
	}

	// check top level is $jsonSchema
	if js, ok := schema["$jsonSchema"]; ok {
		schema = js.(bson.M)
	}

	// bsonType, required, properties at top level
	// looking for properties field, specifically
	if properties, ok := schema["properties"]; ok {
		properties := properties.(bson.M)
		s.iterateProperties("", properties)
	}
}

func (s *Schema) iterateProperties(parentPrefix string, properties bson.M) {
	// iterate each field within properties
	for field, value := range properties {
		switch value := value.(type) {
		case bson.M:
			// retrieve the type of the field
			if bsonType, ok := value["bsonType"]; ok {
				bsonType := bsonType.(string)
				// capture type in the fieldTypes map
				if bsonType != "" {
					s.FieldTypes[fmt.Sprintf("%s%s", parentPrefix, field)] = bsonType
				}

				if bsonType == "array" {
					// look at "items"
					if items, ok := value["items"]; ok {
						value = items.(bson.M)
					}
				}

				if subProperties, ok := value["properties"]; ok {
					subProperties := subProperties.(bson.M)
					s.iterateProperties(
						fmt.Sprintf("%s%s.", parentPrefix, field),
						subProperties,
					)
				}

				continue
			}

			// check for enum (without bsonType specified)
			if _, ok := value["enum"]; ok {
				s.FieldTypes[fmt.Sprintf("%s%s", parentPrefix, field)] = "object"
			}
		default:
			// unknown type
			continue
		}
	}
}
