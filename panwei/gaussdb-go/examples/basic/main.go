package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	gaussdbgo "github.com/lwplvx/gorm-dm/panwei/gaussdb-go"
)

func main() {
	fmt.Println("\uF4B3 GaussDB CRUD Demo")
	fmt.Println("=======================================")
	// 获取连接字符串
	connStr := os.Getenv("DATABASE_URL")
	if connStr == "" {
		connStr = "host=localhost user=gaussdb password=Gaussdb@123 database=postgres port=5433"
	}

	fmt.Println("🔗 Connecting to GaussDB...")
	ctx := context.Background()

	// 建立连接
	conn, err := gaussdbgo.Connect(ctx, connStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(ctx)

	fmt.Println("✅ Connected successfully!")
	fmt.Println("📊 Database connection info:")
	fmt.Println("   - Using gassdb-go driver")
	fmt.Println("   - Connection string:", maskPassword(connStr))

	// 创建表
	fmt.Println("\n🏗️  Creating test table...")
	_, err = conn.Exec(ctx, `
		DROP TABLE IF EXISTS example_users;
		CREATE TABLE example_users (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			age INTEGER,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		);
	`)
	if err != nil {
		panic(err)
	}
	fmt.Println("✅ Table created successfully!")

	// 插入数据
	fmt.Println("\n📝 Inserting test data...")
	users := []struct {
		name  string
		email string
		age   int
	}{
		{"Alice Johnson", "alice@example.com", 28},
		{"Bob Smith", "bob@example.com", 35},
		{"Charlie Brown", "charlie@example.com", 42},
		{"Diana Prince", "diana@example.com", 30},
	}

	for _, u := range users {
		_, err := conn.Exec(ctx,
			"INSERT INTO example_users (name, email, age) VALUES ($1, $2, $3)",
			u.name, u.email, u.age,
		)
		if err != nil {
			panic(err)
		}
		fmt.Printf("   ✓ Inserted: %s (%s, age %d)\n", u.name, u.email, u.age)
	}

	// 查询全部用户
	fmt.Println("\n📖 Querying all users...")
	rows, err := conn.Query(ctx, "SELECT id, name, email, age, created_at FROM example_users ORDER BY id")
	if err != nil {
		panic(err)
	}
	defer rows.Close()

	fmt.Println("   ┌─────┬─────────────────┬─────────────────────┬─────┬─────────────────────┐")
	fmt.Println("   │ ID  │ Name            │ Email               │ Age │ Created At          │")
	fmt.Println("   ├─────┼─────────────────┼─────────────────────┼─────┼─────────────────────┤")

	for rows.Next() {
		var id, age int
		var name, email string
		var createdAt time.Time
		err := rows.Scan(&id, &name, &email, &age, &createdAt)
		if err != nil {
			panic(err)
		}
		fmt.Printf("   │ %3d │ %-15s │ %-19s │ %3d │ %-19s │\n",
			id, name, email, age, createdAt.Format("2006-01-02 15:04:05"))
	}
	fmt.Println("   └─────┴─────────────────┴─────────────────────┴─────┴─────────────────────┘")

	// 使用预编译查询
	fmt.Println("\n🔍 Using prepared statements...")
	stmt, err := conn.Prepare(ctx, "select_older_than", "SELECT name, email FROM example_users WHERE age > $1")
	if err != nil {
		panic(err)
	}
	rows, err = conn.Query(ctx, stmt.Name, 30)
	if err != nil {
		panic(err)
	}
	fmt.Println("   Users older than 30:")
	for rows.Next() {
		var name, email string
		err := rows.Scan(&name, &email)
		if err != nil {
			panic(err)
		}
		fmt.Printf("   - %s (%s)\n", name, email)
	}

	// 查询单个用户
	fmt.Println("\n👤 Finding specific user...")
	var name string
	var age int
	err = conn.QueryRow(ctx, "SELECT name, age FROM example_users WHERE email = $1", "alice@example.com").Scan(&name, &age)
	if err == nil {
		fmt.Printf("   Found user: %s is %d years old\n", name, age)
	} else {
		fmt.Printf("   User not found: %v\n", err)
	}

	// 可选查询
	fmt.Println("\n🔍 Optional query (may not find result)...")
	err = conn.QueryRow(ctx, "SELECT name FROM example_users WHERE email = $1", "nonexistent@example.com").Scan(&name)
	if err == gaussdbgo.ErrNoRows {
		fmt.Println("   No user found with that email")
	} else if err != nil {
		fmt.Printf("   Query error: %v\n", err)
	} else {
		fmt.Printf("   Found user: %s\n", name)
	}

	// 更新数据
	fmt.Println("\n✏️  Updating user data...")
	tag, err := conn.Exec(ctx, "UPDATE example_users SET age = age + 1 WHERE name = $1", "Alice Johnson")
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Updated %d row(s)\n", tag.RowsAffected())

	// 验证更新
	err = conn.QueryRow(ctx, "SELECT age FROM example_users WHERE name = $1", "Alice Johnson").Scan(&age)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Alice's new age: %d\n", age)

	// 删除数据
	fmt.Println("\n🗑️  Cleaning up...")
	tag, err = conn.Exec(ctx, "DELETE FROM example_users WHERE age > $1", 40)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Deleted %d user(s) older than 40\n", tag.RowsAffected())

	// 查询总数
	err = conn.QueryRow(ctx, "SELECT COUNT(*) FROM example_users").Scan(&age)
	if err != nil {
		panic(err)
	}
	fmt.Printf("   Remaining users: %d\n", age)

	// 删除表
	_, err = conn.Exec(ctx, "DROP TABLE example_users")
	if err != nil {
		panic(err)
	}
	fmt.Println("   ✅ Test table dropped")

	fmt.Println("\n🎉 Basic example completed successfully!")
}

func maskPassword(connStr string) string {
	parts := strings.Fields(connStr)
	for i, part := range parts {
		if strings.HasPrefix(part, "password=") {
			parts[i] = "password=***"
		}
	}
	return strings.Join(parts, " ")
}
