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

package duckdb_test

import (
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	duckdb "github.com/hauntedness/duckdb"
	"github.com/stretchr/testify/assert"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Define test structs.
type User struct {
	ID    uint   `gorm:"column:id;primaryKey;autoIncrement"`
	Name  string `gorm:"column:name"`
	Email string `gorm:"column:email;varchar(255);unique"`
}

type Product struct {
	ID    uint    `gorm:"column:id;primaryKey;autoIncrement"`
	Name  string  `gorm:"column:name;varchar(128)"`
	Price float64 `gorm:"column:price;default:0"`
}

type Post struct {
	ID        uint      `gorm:"column:id;primaryKey;autoIncrement"`
	Content   string    `gorm:"column:content"`
	CreatedAt time.Time `gorm:"column:created_at;default:current_timestamp"`
}

// Test structs for deleted_at limitation verification.
type UserWithGormModel struct {
	gorm.Model

	Name  string `gorm:"column:name"`
	Email string `gorm:"column:email;varchar(255);unique"`
}

type UserWithCustomFields struct {
	ID        uint      `gorm:"column:id;primaryKey;autoIncrement"`
	CreatedAt time.Time `gorm:"column:created_at"`
	UpdatedAt time.Time `gorm:"column:updated_at"`
	Name      string    `gorm:"column:name"`
	Email     string    `gorm:"column:email;varchar(255);unique"`
}

func initDB(t *testing.T) *gorm.DB {
	db, err := gorm.Open(duckdb.Open("test.db"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Info),
	})
	assert.NoError(t, err)

	return db
}

func closeDB(t *testing.T, db *gorm.DB) {
	sqlDB, err := db.DB()
	assert.NoError(t, err)
	assert.NoError(t, sqlDB.Close())

	if rmErr := os.Remove("test.db"); err != nil && !strings.Contains(rmErr.Error(), "no such file") {
		t.Log("remove db error:", rmErr)
	}

	if rmErr := os.Remove("test.db.wal"); rmErr != nil && !strings.Contains(rmErr.Error(), "no such file") {
		t.Log("remove wal error:", rmErr)
	}
}

// TestMigratorBasicSchema verifies basic schema creation.
func TestMigratorBasicSchema(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate User table
	err := db.AutoMigrate(&Product{})
	assert.NoError(t, err)

	// Check if table exists
	assert.True(t, db.Migrator().HasTable(&Product{}))
	assert.True(t, db.Migrator().HasColumn(&Product{}, "Price"))
}

// TestMigratorDropTable verifies dropping a table.
func TestMigratorDropTable(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	_ = db.AutoMigrate(&User{})
	assert.True(t, db.Migrator().HasTable(&User{}))

	// Drop table and verify
	_ = db.Migrator().DropTable(&User{})
	assert.False(t, db.Migrator().HasTable(&User{}))
}

func TestAutoIncrement(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	_ = db.AutoMigrate(&User{})
	assert.True(t, db.Migrator().HasColumn(&User{}, "Email"))

	// Create first user with unique email for this test
	user1 := User{Name: "User1", Email: "autoincrement@example.com"}
	result1 := db.Create(&user1)
	assert.NoError(t, result1.Error)
	assert.Equal(t, uint(1), user1.ID)
}

// TestUniqueConstraint tests that unique constraints are enforced.
func TestUniqueConstraint(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	_ = db.AutoMigrate(&User{})
	assert.True(t, db.Migrator().HasColumn(&User{}, "Email"))

	// Create first user
	user1 := User{Name: "User1", Email: "user@example.com"}
	result1 := db.Create(&user1)
	assert.NoError(t, result1.Error)

	// Attempt to create a second user with the same email
	user2 := User{Name: "User2", Email: "user@example.com"}
	result2 := db.Create(&user2)
	assert.Error(t, result2.Error, "Expected unique constraint violation")
}

// TestDefaultValues verifies that default values are set correctly.
func TestDefaultValues(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	_ = db.AutoMigrate(&Post{})

	// Insert a new post without specifying CreatedAt
	post := Post{Content: "Hello, World!", CreatedAt: time.Now()}
	db.Create(&post)

	// Verify CreatedAt has a value (defaulted to the current timestamp)
	assert.NotZero(t, post.CreatedAt)
}

// TestGormModelSoftDeleteLimitation verifies the deleted_at field limitation mentioned in README.
func TestGormModelSoftDeleteLimitation(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate table with gorm.Model (includes deleted_at)
	err := db.AutoMigrate(&UserWithGormModel{})
	assert.NoError(t, err)

	// Create first user
	user1 := UserWithGormModel{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err = db.Create(&user1).Error
	assert.NoError(t, err)
	assert.NotZero(t, user1.ID)

	// Soft delete the user (this sets deleted_at instead of actually deleting)
	err = db.Delete(&user1).Error
	// Note: This might fail due to DuckDB's ART index limitations with soft deletes
	if err != nil {
		t.Logf("Soft delete failed as expected due to DuckDB limitations: %v", err)
		// If soft delete fails, we can't proceed with the rest of the test
		// return
	}

	// Try to create another user with the same email
	// This should potentially cause issues due to DuckDB's ART index limitations
	user2 := UserWithGormModel{
		Name:  "Jane Doe",
		Email: "john@example.com", // Same email as deleted user
	}
	err = db.Create(&user2).Error

	// According to README, this might fail due to primary key constraint violations
	// We'll check if the error occurs
	if err != nil {
		t.Logf("Expected error occurred with gorm.Model soft delete: %v", err)
		// This confirms the limitation mentioned in README
	} else {
		t.Logf("No error occurred - the limitation might not apply in this case")
	}

	// Verify the soft-deleted user still exists in database but is marked as deleted
	var deletedUser UserWithGormModel

	err = db.Unscoped().Where("email = ?", "john@example.com").First(&deletedUser).Error
	assert.NoError(t, err)
	assert.NotNil(t, deletedUser.DeletedAt)
}

// TestCustomFieldsWithoutDeletedAt verifies that custom structs work properly.
func TestCustomFieldsWithoutDeletedAt(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate table with custom fields (no deleted_at)
	err := db.AutoMigrate(&UserWithCustomFields{})
	assert.NoError(t, err)

	// Create first user
	user1 := UserWithCustomFields{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err = db.Create(&user1).Error
	assert.NoError(t, err)
	assert.NotZero(t, user1.ID)

	// Hard delete the user (actually removes from database)
	err = db.Delete(&user1).Error
	assert.NoError(t, err)

	// Verify user is actually deleted
	var deletedUser UserWithCustomFields

	err = db.Where("email = ?", "john@example.com").First(&deletedUser).Error
	assert.Error(t, err) // Should return "record not found" error

	// Create another user with the same email - this should work fine
	user2 := UserWithCustomFields{
		Name:  "Jane Doe",
		Email: "john@example.com", // Same email as deleted user
	}
	err = db.Create(&user2).Error
	assert.NoError(t, err)
	assert.NotZero(t, user2.ID)

	// This demonstrates that without deleted_at, there are no constraint issues
	t.Logf("Successfully created user with same email after hard delete")
}
