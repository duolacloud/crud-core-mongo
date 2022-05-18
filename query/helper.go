package query

func getSchemaKey(key string) string {
	if key == "id" {
		return "_id"
	}
	return key
}
