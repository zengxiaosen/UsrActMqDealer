package jsonUtils

import (
	"fmt"
	"github.com/W1llyu/ourjson"
)

func main() {
	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	jsonStr := `{
        "user": {
            "name": "aa",
            "age": 10,
            "phone": "12222222222",
            "emails": [
                "aa@164.com",
                "aa@165.com"
            ],
            "address": [
                {
                    "number": "101",
                    "now_live": true
                },
                {
                    "number": "102",
                    "now_live": null
                }
            ],
            "account": {
                "balance": 999.9
            }
        }
    }
    `
	jsonObject, err := ourjson.ParseObject(jsonStr)
	fmt.Println(jsonObject, err)

	user := jsonObject.GetJsonObject("user")
	fmt.Println(user)

	name, err := user.GetString("name")
	fmt.Println(name, err)

}
