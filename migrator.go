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
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

var ErrDuckDBNotSupported = errors.New("DuckDB are not supported this operation")
var ErrInvalidAssert = errors.New("Invalid type assertion")

var typeAliasMap = map[string][]string{
	"int":                      {"integer"},
	"int2":                     {"smallint"},
	"int4":                     {"integer"},
	"int8":                     {"bigint"},
	"smallint":                 {"int2"},
	"integer":                  {"int4"},
	"bigint":                   {"int8"},
	"decimal":                  {"numeric"},
	"numeric":                  {"decimal"},
	"timestamptz":              {"timestamp with time zone"},
	"timestamp with time zone": {"timestamptz"},
	"bool":                     {"boolean"},
	"boolean":                  {"bool"},
	"bit":                      {"bitstring"},
	"char":                     {"character"},
	"varchar":                  {"character varying"},
	"float4":                   {"real"},
	"float8":                   {"double"},
	"blob":                     {"binary"},
}

type Migrator struct {
	migrator.Migrator
}

type BuildIndexOptionsInterface interface {
	BuildIndexOptions([]schema.IndexOption, *gorm.Statement) []any
}

// Database

func (m Migrator) CurrentDatabase() (name string) {
	m.DB.Raw("SELECT CURRENT_DATABASE()").Scan(&name)
	return
}

func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
	expr := m.Migrator.FullDataTypeOf(field)

	if value, ok := field.TagSettings["COMMENT"]; ok {
		if dialector, ok := m.DB.Dialector.(Dialector); ok {
			expr.SQL += " COMMENT " + dialector.Explain("?", value)
		}
	}

	return expr
}

func (m Migrator) GetTypeAliases(databaseTypeName string) []string {
	return typeAliasMap[databaseTypeName]
}

// Tables

func (m Migrator) createSequence(values ...any) error {
	for _, value := range m.ReorderModels(values, false) {
		err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema != nil {
				// Check if schema has an id field
				if dbName, ok := m.autoIncrementPrimaryKey(stmt, nil); ok {
					sequenceName, err := m.idSequenceName(stmt, dbName)
					if err != nil {
						return err
					}

					csql := "CREATE SEQUENCE IF NOT EXISTS " + sequenceName + " START 1"
					if err := m.DB.Exec(csql).Error; err != nil {
						return err
					}
				}
			}

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (m Migrator) CreateTable(values ...any) (err error) {
	if err := m.createSequence(values...); err != nil {
		return err
	}

	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
			if stmt.Schema == nil {
				return errors.New("failed to get schema")
			}

			var (
				createTableSQL          = "CREATE TABLE ? ("
				values                  = []any{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			var createTableSQLSb146 strings.Builder
			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					if _, ok := m.autoIncrementPrimaryKey(stmt, &dbName); ok {
						sequenceName, err := m.idSequenceName(stmt, dbName)
						if err != nil {
							return err
						}
						pk := fmt.Sprintf("? ? DEFAULT nextval('%s')", sequenceName)
						createTableSQLSb146.WriteString(pk)
					} else {
						createTableSQLSb146.WriteString("? ?")
					}
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(m.DataTypeOf(field)), "PRIMARY KEY")
					values = append(values, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
					createTableSQLSb146.WriteString(",")
				}
			}
			createTableSQL += createTableSQLSb146.String()

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				primaryKeys := make([]any, 0, len(stmt.Schema.PrimaryFields))
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}

				values = append(values, primaryKeys)
			}

			var createTableSQLSb174 strings.Builder
			for _, idx := range stmt.Schema.ParseIndexes() {
				if m.CreateIndexAfterCreateTable {
					defer func(value any, name string) {
						if err == nil {
							err = tx.Migrator().CreateIndex(value, name)
						}
					}(value, idx.Name)
				} else {
					if idx.Class != "" {
						createTableSQLSb174.WriteString(idx.Class + " ")
					}
					createTableSQLSb174.WriteString("INDEX ? ?")

					if idx.Comment != "" {
						createTableSQLSb174.WriteString(fmt.Sprintf(" COMMENT '%s'", idx.Comment))
					}

					if idx.Option != "" {
						createTableSQLSb174.WriteString(" " + idx.Option)
					}

					createTableSQLSb174.WriteString(",")
					values = append(values, clause.Column{Name: idx.Name}, tx.Migrator().(BuildIndexOptionsInterface).BuildIndexOptions(idx.Fields, stmt))
				}
			}
			createTableSQL += createTableSQLSb174.String()

			if !m.DB.DisableForeignKeyConstraintWhenMigrating && !m.DB.IgnoreRelationshipsWhenMigrating {
				var createTableSQLSb201 strings.Builder
				for _, rel := range stmt.Schema.Relationships.Relations {
					if rel.Field.IgnoreMigration {
						continue
					}
					if constraint := rel.ParseConstraint(); constraint != nil {
						if constraint.Schema == stmt.Schema {
							sql, vars := constraint.Build()
							createTableSQLSb201.WriteString(sql + ",")
							values = append(values, vars...)
						}
					}
				}
				createTableSQL += createTableSQLSb201.String()
			}

			var createTableSQLSb215 strings.Builder
			for _, uni := range stmt.Schema.ParseUniqueConstraints() {
				createTableSQLSb215.WriteString("CONSTRAINT ? UNIQUE (?),")
				values = append(values, clause.Column{Name: uni.Name}, clause.Expr{SQL: stmt.Quote(uni.Field.DBName)})
			}
			createTableSQL += createTableSQLSb215.String()

			var createTableSQLSb220 strings.Builder
			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				createTableSQLSb220.WriteString("CONSTRAINT ? CHECK (?),")
				values = append(values, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
			}
			createTableSQL += createTableSQLSb220.String()

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")

			createTableSQL += ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}

			err = tx.Exec(createTableSQL, values...).Error
			return err
		}); err != nil {
			return err
		}
	}

	for _, value := range m.ReorderModels(values, false) {
		if err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
			if stmt.Schema != nil {
				for _, fieldName := range stmt.Schema.DBNames {
					field := stmt.Schema.FieldsByDBName[fieldName]
					if field.Comment != "" {
						if err := m.DB.Exec(
							"COMMENT ON COLUMN ?.? IS ?",
							m.CurrentTable(stmt), clause.Column{Name: field.DBName}, gorm.Expr(m.Migrator.Dialector.Explain("$1", field.Comment)),
						).Error; err != nil {
							return err
						}
					}
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return err
}

func (m Migrator) DropTable(values ...any) error {
	values = m.ReorderModels(values, false)

	tx := m.DB.Session(&gorm.Session{})
	for i := len(values) - 1; i >= 0; i-- {
		if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
			return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", m.CurrentTable(stmt)).Error
		}); err != nil {
			return err
		}
	}

	return nil
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (any, any) {
	if strings.Contains(table, ".") {
		if tables := strings.Split(table, `.`); len(tables) == 2 {
			return tables[0], tables[1]
		}
	}

	if stmt.TableExpr != nil {
		if tables := strings.Split(stmt.TableExpr.SQL, `"."`); len(tables) == 2 {
			return strings.TrimPrefix(tables[0], `"`), table
		}
	}

	return clause.Expr{SQL: "CURRENT_SCHEMA()"}, table
}

func (m Migrator) HasTable(value any) bool {
	var count int64

	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentSchema, curTable := m.CurrentSchema(stmt, stmt.Table)
		return m.DB.Raw("SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ? AND table_type = ?", currentSchema, curTable, "BASE TABLE").Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) RenameTable(oldName, newName any) (err error) {
	resolveTable := func(name any) (result string, err error) {
		if v, ok := name.(string); ok {
			result = v
		} else {
			stmt := &gorm.Statement{DB: m.DB}
			if err = stmt.Parse(name); err == nil {
				result = stmt.Table
			}
		}

		return
	}

	var oldTable, newTable string

	if oldTable, err = resolveTable(oldName); err != nil {
		return err
	}

	if newTable, err = resolveTable(newName); err != nil {
		return err
	}

	if !m.HasTable(oldTable) {
		return err
	}

	return m.DB.Exec("RENAME TABLE ? TO ?",
		clause.Table{Name: oldTable},
		clause.Table{Name: newTable},
	).Error
}

func (m Migrator) GetTables() (tableList []string, err error) {
	currentSchema, _ := m.CurrentSchema(m.DB.Statement, "")
	return tableList, m.DB.Raw("SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = ?", currentSchema, "BASE TABLE").Scan(&tableList).Error
}

// Columns.
func (m Migrator) DropColumn(dst any, field string) error {
	if err := m.Migrator.DropColumn(dst, field); err != nil {
		return err
	}

	m.resetPreparedStmts()

	return nil
}

// should reset prepared stmts when table changed
// https://duckdb.org/docs/sql/query_syntax/prepared_statements.html
func (m Migrator) resetPreparedStmts() {
	if m.DB.PrepareStmt {
		if pdb, ok := m.DB.ConnPool.(*gorm.PreparedStmtDB); ok {
			pdb.Reset()
		}
	}
}

func (m Migrator) MigrateColumn(value any, field *schema.Field, columnType gorm.ColumnType) error {
	// skip primary field and unique fields as DuckDB doesn't support altering column types with constraints
	if !field.PrimaryKey && !field.Unique {
		if err := m.Migrator.MigrateColumn(value, field, columnType); err != nil {
			return err
		}
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var description sql.NullString

		currentSchema, curTable := m.CurrentSchema(stmt, stmt.Table)
		values := []any{currentSchema, curTable, field.DBName, stmt.Table, currentSchema}
		checkSQL := "SELECT description FROM pg_catalog.pg_description "
		checkSQL += "WHERE objsubid IN (SELECT ordinal_position FROM information_schema.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?) "
		checkSQL += "AND objoid IN (SELECT oid FROM pg_catalog.pg_class WHERE relname = ? AND relnamespace IN "

		checkSQL += "(SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = ?))"
		if err := m.DB.Raw(checkSQL, values...).Row().Scan(&description); err != nil {
			return err
		}

		comment := strings.Trim(field.Comment, "'")

		comment = strings.Trim(comment, `"`)
		if field.Comment != "" && comment != description.String {
			if err := m.DB.Exec(
				"COMMENT ON COLUMN ?.? IS ?",
				m.CurrentTable(stmt), clause.Column{Name: field.DBName}, gorm.Expr(func() string {
					if dialector, ok := m.DB.Dialector.(Dialector); ok {
						return dialector.Explain("$1", field.Comment)
					}
					return "?"
				}()),
			).Error; err != nil {
				return err
			}
		}

		return nil
	})
}

func (m Migrator) HasColumn(value any, field string) bool {
	var count int64

	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		name := field
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				name = field.DBName
			}
		}

		currentSchema, curTable := m.CurrentSchema(stmt, stmt.Table)

		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.columns WHERE table_schema = ? AND table_name = ? AND column_name = ?",
			currentSchema, curTable, name,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) RenameColumn(dst any, oldName, field string) error {
	if err := m.Migrator.RenameColumn(dst, oldName, field); err != nil {
		return err
	}

	m.resetPreparedStmts()

	return nil
}

// TODO: Implement below function.
// func (m Migrator) ColumnTypes(value interface{}) (columnTypes []gorm.ColumnType, err error)

// Views.
func (m Migrator) CreateView(name string, option gorm.ViewOption) error {
	return ErrDuckDBNotSupported
}

func (m Migrator) DropView(name string) error {
	return ErrDuckDBNotSupported
}

// Constraints

// WARNING: Constraints have a strong impact on performance:
// they slow down loading and updates but speed up certain queries.
// https://duckdb.org/docs/guides/performance/schema.html#constraints

func (m Migrator) HasConstraint(value any, name string) bool {
	var count int64

	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, table := m.GuessConstraintInterfaceAndTable(stmt, name)
		if constraint != nil {
			name = constraint.GetName()
		}

		currentSchema, curTable := m.CurrentSchema(stmt, table)

		return m.DB.Raw(
			"SELECT count(*) FROM INFORMATION_SCHEMA.table_constraints WHERE table_schema = ? AND table_name = ? AND constraint_name = ?",
			currentSchema, curTable, name,
		).Scan(&count).Error
	})

	return count > 0
}

// https://duckdb.org/docs/sql/statements/alter_table.html#add--drop-constraint
func (m Migrator) DropConstraint(dst any, name string) error {
	return ErrDuckDBNotSupported
}

// TODO: Implement below function.
// func (m Migrator) CreateConstraint(value interface{}, name string) error {}

// Indexes

func (m Migrator) CreateIndex(value any, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				opts := m.BuildIndexOptions(idx.Fields, stmt)
				values := []any{clause.Column{Name: idx.Name}, m.CurrentTable(stmt), opts}

				createIndexSQL := "CREATE "
				if idx.Class != "" {
					createIndexSQL += idx.Class + " "
				}

				createIndexSQL += "INDEX IF NOT EXISTS ? ON ?"

				if idx.Type != "" {
					createIndexSQL += " USING " + idx.Type + "(?)"
				} else {
					createIndexSQL += " ?"
				}

				err := m.DB.Exec(createIndexSQL, values...).Error
				if err != nil {
					return err
				}

				if !m.HasIndex(value, name) {
					return fmt.Errorf("failed to create index with name %v", name)
				}

				return nil
			}
		}

		return fmt.Errorf("failed to create index with name %v", name)
	})
}

func (m Migrator) DropIndex(value any, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}

		return m.DB.Exec("DROP INDEX IF EXISTS ?", clause.Column{Name: name}).Error
	})
}

func (m Migrator) RenameIndex(dst any, oldName, newName string) error {
	return ErrDuckDBNotSupported
}

func (m Migrator) HasIndex(value any, name string) bool {
	var count int64

	_ = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(name); idx != nil {
				name = idx.Name
			}
		}

		currentSchema, curTable := m.CurrentSchema(stmt, stmt.Table)

		return m.DB.Raw(
			"SELECT count(*) FROM pg_indexes WHERE tablename = ? AND indexname = ? AND schemaname = ?", curTable, name, currentSchema,
		).Scan(&count).Error
	})

	return count > 0
}

func (m Migrator) autoIncrementPrimaryKey(stmt *gorm.Statement, dbname *string) (string, bool) {
	for _, field := range stmt.Schema.Fields {
		if dbname != nil && *dbname != field.DBName {
			return "", false
		}

		if field.AutoIncrement && field.PrimaryKey {
			return field.DBName, true
		}
	}

	return "", false
}

func (m Migrator) idSequenceName(stmt *gorm.Statement, dbName string) (string, error) {
	currentTable, ok := m.CurrentTable(stmt).(clause.Table)
	if !ok {
		return "", ErrInvalidAssert
	}

	tableName := currentTable.Name
	sequenceName := tableName + "_" + dbName + "_seq"

	return sequenceName, nil
}
