# GORM DuckDB Driver

[![Go Reference](https://pkg.go.dev/badge/github.com/hauntedness/duckdb/v2.svg)](https://pkg.go.dev/github.com/hauntedness/duckdb/v2)
[![Go version](https://img.shields.io/github/go-mod/go-version/vogo/duckdb?logo=go)](https://github.com/hauntedness/duckdb/v2)
[![GitHub release](https://img.shields.io/github/v/release/vogo/duckdb)](https://github.com/hauntedness/duckdb/v2/releases)
[![Go Report Card](https://goreportcard.com/badge/github.com/hauntedness/duckdb/v2)](https://goreportcard.com/report/github.com/hauntedness/duckdb/v2)
[![License](https://img.shields.io/github/license/vogo/duckdb?&color=blue)](https://github.com/hauntedness/duckdb/v2/blob/main/LICENSE)


---

## Quick Start

```go
import (
  "github.com/hauntedness/duckdb/v2"
  "gorm.io/gorm"
)

type Product struct {
	ID        uint `gorm:"primarykey"`
	Code      string
	Price     uint
	CreatedAt time.Time
	UpdatedAt time.Time
}

func main() {
	db, err := gorm.Open(duckdb.Open("duckdb.ddb"), &gorm.Config{})
	if err != nil {
		panic("failed to connect database")
	}

	db.AutoMigrate(&Product{})

	// Create
	db.Create(&Product{Code: "D42", Price: 100})

	// Read
	var product Product
	db.First(&product, 1)
	db.First(&product, "code = ?", "D42")

	// Update
	db.Model(&product).Update("Price", 200)
	db.Model(&product).Updates(Product{Price: 200, Code: "F42"})

	// Delete
	db.Delete(&product, 1)
}
```

Checkout [https://gorm.io](https://gorm.io) for details.


## Limitations

#### `deleted_at` field

**Do not use `gorm.Model`** - use custom struct with `ID`, `CreatedAt`, `UpdatedAt` fields instead.

DuckDB's ART indexes have limitations with soft deletes. When GORM performs `db.Delete()`, it updates the `deleted_at` field instead of actually deleting the record, which can cause primary key constraint violations due to how DuckDB handles transactions and indexes.

See [DuckDB documentation](https://duckdb.org/docs/sql/constraints#primary-key-and-unique-constraint) for details.


## Contributing

Any contributions you make are **greatly appreciated**.

## License

This project is licensed under the [Apache License 2.0](LICENSE).

