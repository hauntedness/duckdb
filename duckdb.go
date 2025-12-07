/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package duckdb

import (
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const DriverName = "duckdb"

type Dialector struct {
	*Config
}

type Config struct {
	DriverName string
	DSN        string
	Conn       gorm.ConnPool
}

func Open(dsn string) gorm.Dialector {
	return &Dialector{&Config{DSN: dsn}}
}

func New(config Config) gorm.Dialector {
	return &Dialector{Config: &config}
}

func (dialector Dialector) Name() string {
	return DriverName
}

func (dialector Dialector) Initialize(db *gorm.DB) (err error) {
	if dialector.DriverName == "" {
		dialector.DriverName = DriverName
	}

	callbacks.RegisterDefaultCallbacks(db, &callbacks.Config{
		CreateClauses: []string{"INSERT", "VALUES", "ON CONFLICT", "RETURNING"},
		UpdateClauses: []string{"UPDATE", "SET", "WHERE", "RETURNING"},
		DeleteClauses: []string{"DELETE", "FROM", "WHERE", "RETURNING"},
	})

	if dialector.Conn != nil {
		db.ConnPool = dialector.Conn
	} else {
		db.ConnPool, err = sql.Open(dialector.DriverName, dialector.DSN)
		if err != nil {
			return err
		}
	}

	var version string
	if err := db.ConnPool.QueryRowContext(context.Background(), "SELECT version()").Scan(&version); err != nil {
		return err
	}

	maps.Copy(db.ClauseBuilders, dialector.ClauseBuilders())

	return
}

func (dialector Dialector) ClauseBuilders() map[string]clause.ClauseBuilder {
	return map[string]clause.ClauseBuilder{
		"INSERT": func(c clause.Clause, builder clause.Builder) {
			if insert, ok := c.Expression.(clause.Insert); ok {
				if stmt, ok := builder.(*gorm.Statement); ok {
					_, _ = stmt.WriteString("INSERT ")
					if insert.Modifier != "" {
						_, _ = stmt.WriteString(insert.Modifier)
						_ = stmt.WriteByte(' ')
					}

					_, _ = stmt.WriteString("INTO ")
					if insert.Table.Name == "" {
						stmt.WriteQuoted(stmt.Table)
					} else {
						stmt.WriteQuoted(insert.Table)
					}

					return
				}
			}

			c.Build(builder)
		},
		"RETURNING": func(c clause.Clause, builder clause.Builder) {
			if returning, ok := c.Expression.(clause.Returning); ok {
				_, _ = builder.WriteString("RETURNING ")

				for idx, column := range returning.Columns {
					if idx > 0 {
						_ = builder.WriteByte(',')
					}

					builder.WriteQuoted(column)
				}
			}
		},
		"LIMIT": func(c clause.Clause, builder clause.Builder) {
			if limit, ok := c.Expression.(clause.Limit); ok {
				lmt := -1
				if limit.Limit != nil && *limit.Limit >= 0 {
					lmt = *limit.Limit
				}

				if lmt >= 0 || limit.Offset > 0 {
					_, _ = builder.WriteString("LIMIT ")
					_, _ = builder.WriteString(strconv.Itoa(lmt))
				}

				if limit.Offset > 0 {
					_, _ = builder.WriteString(" OFFSET ")
					_, _ = builder.WriteString(strconv.Itoa(limit.Offset))
				}
			}
		},
		"ON CONFLICT_BACKUP": func(c clause.Clause, builder clause.Builder) {
			if onConflict, ok := c.Expression.(clause.OnConflict); ok {
				// for now only support (DO NOTHING or DO UPDATE ALL)
				if onConflict.DoNothing {
					if len(onConflict.Columns) > 0 {
						_, _ = builder.WriteString("ON CONFLICT")
						_ = builder.WriteByte(' ')
						_ = builder.WriteByte('(')

						for idx, column := range onConflict.Columns {
							if idx > 0 {
								_ = builder.WriteByte(',')
							}

							builder.WriteQuoted(column)
						}

						_ = builder.WriteByte(')')
						_, _ = builder.WriteString(" DO NOTHING")
					}
				} else if onConflict.UpdateAll {
					stmt, ok := builder.(*gorm.Statement)
					if ok && stmt.Schema != nil {
						if len(onConflict.Columns) > 0 {
							_, _ = builder.WriteString("ON CONFLICT")
							_ = builder.WriteByte(' ')
							_ = builder.WriteByte('(')

							for idx, column := range onConflict.Columns {
								if idx > 0 {
									_ = builder.WriteByte(',')
								}

								builder.WriteQuoted(column)
							}

							_ = builder.WriteByte(')')

							if slices.ContainsFunc(stmt.Schema.Fields, containUpdatable) {
								_, _ = builder.WriteString(" DO UPDATE SET ")
								written := false

								for _, field := range stmt.Schema.Fields {
									if !field.PrimaryKey && field.Updatable {
										if written {
											_ = builder.WriteByte(',')
										}

										builder.WriteQuoted(clause.Column{Name: field.DBName})
										_ = builder.WriteByte('=')
										_, _ = builder.WriteString("EXCLUDED.")
										builder.WriteQuoted(clause.Column{Name: field.DBName})

										written = true
									}
								}
							} else {
								_, _ = builder.WriteString(" DO NOTHING")
							}
						}
					}
				}
			}
		},
	}
}

func (dialector Dialector) DefaultValueOf(field *schema.Field) clause.Expression {
	return clause.Expr{SQL: "DEFAULT"}
}

func (dialector Dialector) SupportReturning() bool {
	return true
}

func (dialector Dialector) Migrator(db *gorm.DB) gorm.Migrator {
	return Migrator{migrator.Migrator{Config: migrator.Config{
		DB:                          db,
		Dialector:                   dialector,
		CreateIndexAfterCreateTable: true,
	}}}
}

func (dialector Dialector) DataTypeOf(field *schema.Field) string {
	switch field.DataType {
	case schema.Bool:
		return "boolean"
	case schema.Int, schema.Uint:
		size := field.Size
		if field.DataType == schema.Uint {
			size++
		}

		if field.AutoIncrement {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		} else {
			switch {
			case size <= 16:
				return "smallint"
			case size <= 32:
				return "integer"
			default:
				return "bigint"
			}
		}
	case schema.Float:
		if field.Precision > 0 {
			if field.Scale > 0 {
				return fmt.Sprintf("numeric(%d, %d)", field.Precision, field.Scale)
			}

			return fmt.Sprintf("numeric(%d)", field.Precision)
		}

		return "decimal"
	case schema.String:
		if field.Size > 0 {
			return fmt.Sprintf("varchar(%d)", field.Size)
		}

		return "text"
	case schema.Time:
		if field.Precision > 0 {
			return fmt.Sprintf("timestamptz(%d)", field.Precision)
		}

		return "timestamptz"
	case schema.Bytes:
		return "blob"
	default:
		if field.Tag.Get("gorm") == "type:jsonb" {
			return "json"
		}

		return dialector.getSchemaCustomType(field)
	}
}

func (dialector Dialector) getSchemaCustomType(field *schema.Field) string {
	sqlType := string(field.DataType)

	if field.AutoIncrement && !strings.Contains(strings.ToLower(sqlType), "integer") {
		size := field.Size
		if field.GORMDataType == schema.Uint {
			size++
		}

		switch {
		case size <= 16:
			sqlType = "smallint"
		case size <= 32:
			sqlType = "integer"
		default:
			sqlType = "bigint"
		}
	}

	return sqlType
}

func (dialector Dialector) BindVarTo(writer clause.Writer, stmt *gorm.Statement, v any) {
	_ = writer.WriteByte('?')
}

func (dialector Dialector) QuoteTo(writer clause.Writer, str string) {
	var (
		underQuoted, selfQuoted bool
		continuousBacktick      int8
		shiftDelimiter          int8
	)

	for _, v := range []byte(str) {
		switch v {
		case '`':
			continuousBacktick++
			if continuousBacktick == 2 {
				_, _ = writer.WriteString("")
				continuousBacktick = 0
			}
		case '.':
			if continuousBacktick > 0 || !selfQuoted {
				shiftDelimiter = 0
				underQuoted = false
				continuousBacktick = 0
				_, _ = writer.WriteString("")
			}

			_ = writer.WriteByte(v)

			continue
		default:
			if shiftDelimiter-continuousBacktick <= 0 && !underQuoted {
				_, _ = writer.WriteString("")
				underQuoted = true

				if selfQuoted = continuousBacktick > 0; selfQuoted {
					continuousBacktick -= 1
				}
			}

			for ; continuousBacktick > 0; continuousBacktick -= 1 {
				_, _ = writer.WriteString("")
			}

			_ = writer.WriteByte(v)
		}

		shiftDelimiter++
	}

	if continuousBacktick > 0 && !selfQuoted {
		_, _ = writer.WriteString("")
	}

	_, _ = writer.WriteString("")
}

func (dialector Dialector) Explain(sql string, vars ...any) string {
	return logger.ExplainSQL(sql, nil, `"`, vars...)
}

func (dialectopr Dialector) SavePoint(tx *gorm.DB, name string) error {
	tx.Exec("SAVEPOINT " + name)
	return nil
}

func (dialectopr Dialector) RollbackTo(tx *gorm.DB, name string) error {
	tx.Exec("ROLLBACK TO SAVEPOINT " + name)
	return nil
}

func containUpdatable(field *schema.Field) bool {
	return !field.PrimaryKey && field.Updatable
}
