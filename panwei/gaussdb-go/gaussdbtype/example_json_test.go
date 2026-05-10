package gaussdbtype_test

import (
	"context"
	"fmt"
	"os"

	gaussdbgo "github.com/lwplvx/gorm-dm/panwei/gaussdb-go"
)

func Example_json() {
	conn, err := gaussdbgo.Connect(context.Background(), os.Getenv(gaussdbgo.EnvGaussdbTestDatabase))
	if err != nil {
		fmt.Printf("Unable to establish connection: %v", err)
		return
	}

	type person struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}

	input := person{
		Name: "John",
		Age:  42,
	}

	var output person

	err = conn.QueryRow(context.Background(), "select $1::json", input).Scan(&output)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println(output.Name, output.Age)
	// Output:
	// John 42
}
