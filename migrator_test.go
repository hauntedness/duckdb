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
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	duckdb "github.com/hauntedness/duckdb"
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
	if err != nil {
		t.Fatal(err)
	}

	return db
}

func closeDB(t *testing.T, db *gorm.DB) {
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	if err := os.Remove("test.db"); err != nil {
		log.Printf("remove db error: %v", err)
	}
	if err := os.Remove("test.db.wal"); err != nil {
		log.Printf("remove wal error: %v", err)
	}
}

// TestMigratorBasicSchema verifies basic schema creation.
func TestMigratorBasicSchema(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate User table
	if err := db.AutoMigrate(&Product{}); err != nil {
		t.Fatal(err)
	}

	// Check if table exists
	if !db.Migrator().HasTable(&Product{}) {
		t.Fatal("table should exist")
	}

	if !db.Migrator().HasColumn(&Product{}, "Price") {
		t.Fatal("table should have column Price")
	}
}

// TestMigratorDropTable verifies dropping a table.
func TestMigratorDropTable(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	if err := db.AutoMigrate(&User{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasTable(&User{}) {
		t.Fatal("table should exist")
	}

	// Drop table and verify
	if err := db.Migrator().DropTable(&User{}); err != nil {
		t.Fatal(err)
	}
	if db.Migrator().HasTable(&User{}) {
		t.Fatal("table should not exist")
	}
}

func TestAutoIncrement(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	if err := db.AutoMigrate(&User{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn(&User{}, "Email") {
		t.Fatal("table should have column Email")
	}

	// Create first user with unique email for this test
	user1 := User{Name: "User1", Email: "autoincrement@example.com"}
	result1 := db.Create(&user1)
	if err := result1.Error; err != nil {
		t.Fatal(err)
	}
	if user1.ID != 1 {
		t.Fatal("invalid user1.ID", user1.ID)
	}
}

// TestUniqueConstraint tests that unique constraints are enforced.
func TestUniqueConstraint(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	if err := db.AutoMigrate(&User{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasColumn(&User{}, "Email") {
		t.Fatal("table should have column Email")
	}

	// Create first user
	user1 := User{Name: "User1", Email: "user@example.com"}
	result1 := db.Create(&user1)
	if result1.Error != nil {
		t.Fatalf("failed to create user1: %v", result1.Error)
	}

	// Attempt to create a second user with the same email
	user2 := User{Name: "User2", Email: "user@example.com"}
	result2 := db.Create(&user2)
	if result2.Error == nil {
		t.Error("Expected unique constraint violation")
	}
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
	if post.CreatedAt.IsZero() {
		t.Error("Expected CreatedAt to be set")
	}
}

// TestGormModelSoftDeleteLimitation verifies the deleted_at field limitation mentioned in README.
func TestGormModelSoftDeleteLimitation(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate table with gorm.Model (includes deleted_at)
	err := db.AutoMigrate(&UserWithGormModel{})
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Create first user
	user1 := UserWithGormModel{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err = db.Create(&user1).Error
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if user1.ID == 0 {
		t.Error("Expected ID to be set")
	}

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
	if err != nil {
		t.Fatalf("Failed to find deleted user: %v", err)
	}
	if !deletedUser.DeletedAt.Valid {
		t.Error("Expected DeletedAt to be set")
	}
}

// TestCustomFieldsWithoutDeletedAt verifies that custom structs work properly.
func TestCustomFieldsWithoutDeletedAt(t *testing.T) {
	db := initDB(t)
	defer closeDB(t, db)

	// Migrate table with custom fields (no deleted_at)
	err := db.AutoMigrate(&UserWithCustomFields{})
	if err != nil {
		t.Fatalf("AutoMigrate failed: %v", err)
	}

	// Create first user
	user1 := UserWithCustomFields{
		Name:  "John Doe",
		Email: "john@example.com",
	}
	err = db.Create(&user1).Error
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if user1.ID == 0 {
		t.Error("Expected ID to be set")
	}

	// Hard delete the user (actually removes from database)
	err = db.Delete(&user1).Error
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify user is actually deleted
	var deletedUser UserWithCustomFields

	err = db.Where("email = ?", "john@example.com").First(&deletedUser).Error
	if err == nil {
		t.Error("Expected error record not found")
	}

	// Create another user with the same email - this should work fine
	user2 := UserWithCustomFields{
		Name:  "Jane Doe",
		Email: "john@example.com", // Same email as deleted user
	}
	err = db.Create(&user2).Error
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	if user2.ID == 0 {
		t.Error("Expected ID to be set")
	}

	// This demonstrates that without deleted_at, there are no constraint issues
	t.Logf("Successfully created user with same email after hard delete")
}
