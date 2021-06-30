package main

import (
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
)

const (
	dialectTest = "postgres"
	dialectDSN  = "postgres://postgres:postgres@localhost:5432/test_partition_test?sslmode=disable"
)

var (
	startDate = time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC)

	once  sync.Once
	logic *Logic
)

// SetupConnection will create sql connection and never close it
// documentation said https://golang.org/pkg/database/sql/#Open
//  The returned DB is safe for concurrent use by multiple goroutines and maintains its own pool of idle connections.
//  Thus, the Open function should be called just once. It is rarely necessary to close a DB.
func SetupConnection() {
	once.Do(func() {
		sqlxConn, err := sqlx.Connect(dialectTest, dialectDSN)
		if err != nil {
			panic(err)
		}

		if err = sqlxConn.Ping(); err != nil {
			panic(err)
		}

		logic = NewLogic(sqlxConn)
	})
}

// BenchmarkNewLog to know how the performance of creating NewLog variable itself.
func BenchmarkNewLog(b *testing.B) {
	date := time.Now()
	for i := 0; i < b.N; i++ {
		NewLog(i, date)
	}
}

// BenchmarkLogic_CreateLogTable benchmark create one table multiple times.
// This to know performance of create table and the result is expected similar to BenchmarkCreatePartitionParentTable.
// In real implementation this query is never happen because we only need to create table once in program startup.
func BenchmarkLogic_CreateLogTable(b *testing.B) {
	SetupConnection()

	for i := 0; i < b.N; i++ {
		err := logic.CreateLogTable()
		if err != nil {
			b.Log(err)
			b.Error(err)
			return
		}
	}
}

// BenchmarkLogic_CreatePartitionParentTable benchmark create parent table partition multiple times.
// This to know performance of create parent table and the result is expected similar to BenchmarkCreateLogTable.
// In real implementation this query is never happen because we only need to create parent table once in program startup.
func BenchmarkLogic_CreatePartitionParentTable(b *testing.B) {
	SetupConnection()

	for i := 0; i < b.N; i++ {
		err := logic.CreatePartitionParentTable()
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkLogic_ChildTableName(b *testing.B) {
	for i := 1; i <= b.N; i++ {
		logic.ChildTableName(startDate)
	}
}

// BenchmarkLogic_CreateNewPartitionChildTable benchmark create child table partition in one specific date multiple times.
// This to know performance of create child table partition query in postgre.
func BenchmarkLogic_CreateNewPartitionChildTable(b *testing.B) {
	SetupConnection()

	err := logic.CreatePartitionParentTable()
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		err = logic.CreateNewPartitionChildTable(startDate)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// BenchmarkLogic_CheckPartitionChildTable check whether partition table is exist or not
func BenchmarkLogic_CheckPartitionChildTable(b *testing.B) {
	SetupConnection()
	tableName := logic.ChildTableName(startDate)
	for i := 1; i <= b.N; i++ {
		err := logic.CheckPartitionChildTable(tableName)
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// BenchmarkLogic_InsertTransactionWithoutPartition is normal operation where it only create table once only then insert log multiple times.
// This also called NewLog each iteration.
func BenchmarkLogic_InsertTransactionWithoutPartition(b *testing.B) {
	SetupConnection()

	err := logic.CreateLogTable()
	if err != nil {
		b.Error(err)
		return
	}

	for i := 1; i <= b.N; i++ {
		err = logic.InsertTransactionWithoutPartition(NewLog(i, startDate))
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// BenchmarkLogic_InsertTransactionPartitioned create parent and child partition table once only.
// Then insert data multiple times into it. Data must reside in one partition only since it called with same date.
// This also called NewLog each iteration.
func BenchmarkLogic_InsertTransactionPartitioned(b *testing.B) {
	SetupConnection()

	err := logic.CreatePartitionParentTable()
	if err != nil {
		b.Error(err)
		return
	}

	err = logic.CreateNewPartitionChildTable(startDate)
	if err != nil {
		b.Error(err)
		return
	}

	for i := 1; i <= b.N; i++ {
		err = logic.InsertTransactionPartitioned(NewLog(i, startDate))
		if err != nil {
			b.Error(err)
			return
		}
	}
}

// BenchmarkLogic_InsertTransactionPartitionedDynamic create parent table once only.
// For each insertion, it will try to create new child partition table to ensure that partition table is exist.
// This means, this function will call CreateNewPartitionChildTable and NewLog each iteration inside function InsertLogPartitionRange.
func BenchmarkLogic_InsertTransactionPartitionedDynamic(b *testing.B) {
	SetupConnection()

	err := logic.CreatePartitionParentTable()
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		err = logic.InsertTransactionPartitionedDynamic(NewLog(i, startDate))
		if err != nil {
			b.Error(err)
			return
		}
	}
}

func BenchmarkLogic_InsertLogPartitionedDynamicCached(b *testing.B) {
	SetupConnection()

	err := logic.CreatePartitionParentTable()
	if err != nil {
		b.Error(err)
		return
	}

	for i := 0; i < b.N; i++ {
		err = logic.InsertTransactionPartitionedDynamicCached(NewLog(i, startDate))
		if err != nil {
			b.Error(err)
			return
		}
	}
}
