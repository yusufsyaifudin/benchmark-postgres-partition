package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/satori/go.uuid"
)

const (
	dialect = "postgres"
	dsn     = "postgres://postgres:postgres@localhost:5432/partitioned_table?sslmode=disable"

	// official doc:
	// This automatically creates a matching index on each partition, and any partitions you create or attach later will also have such an index. An index or unique constraint declared on a partitioned table is “virtual” in the same way that the partitioned table is: the actual data is in child indexes on the individual partition tables.
	// -- for faster select and ensure id is unique in selected date
	sqlCreateTableTrxPartitioned = `CREATE TABLE IF NOT EXISTS "transactions_partitioned" (
		"id" VARCHAR NOT NULL,
		"user_id" VARCHAR NOT NULL,
		"info" VARCHAR NOT NULL,
		"status" VARCHAR NOT NULL,
		"trx_date" DATE NOT NULL,
		"trx_timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY ("id", "trx_date")
	) PARTITION BY LIST(trx_date);

	
	CREATE INDEX IF NOT EXISTS idx_transactions_partitioned_id_trx_date_timestamp ON transactions_partitioned (id, user_id, trx_date, trx_timestamp DESC);
	`

	// -- for faster select and ensure id is only for selected date
	sqlCreateTableTrx = `CREATE TABLE IF NOT EXISTS "transactions" (
		"id" VARCHAR NOT NULL,
		"user_id" VARCHAR NOT NULL,
		"info" VARCHAR NOT NULL,
		"status" VARCHAR NOT NULL,
		"trx_date" DATE NOT NULL,
		"trx_timestamp" TIMESTAMP WITH TIME ZONE NOT NULL,
		PRIMARY KEY ("id", "trx_date")
	);

	CREATE INDEX IF NOT EXISTS idx_transactions_id_trx_date_timestamp ON transactions (id, user_id, trx_date, trx_timestamp DESC);
	`
)

func main() {
	sqlxConn, err := sqlx.Connect(dialect, dsn)
	if err != nil {
		log.Fatal(err)
		return
	}

	defer func() {
		if _err := sqlxConn.Close(); _err != nil {
			log.Println(_err)
		}
	}()

	if err = sqlxConn.Ping(); err != nil {
		log.Fatal(err)
		return
	}

	logic := NewLogic(sqlxConn)
	err = logic.CreateLogTable()
	if err != nil {
		log.Fatal(err)
		return
	}

	err = logic.CreatePartitionParentTable()
	if err != nil {
		log.Fatal(err)
		return
	}

	handler := &Handler{
		Logic: logic,
	}

	router := chi.NewRouter()
	router.Post("/partition", handler.PostTransactionPartitioned)
	router.Get("/partition/{id}/{user_id}/{date}", handler.GetOneTransactionPartitioned)

	router.Post("/no-partition", handler.PostTransactionNoPartition)
	router.Get("/no-partition/{id}/{user_id}/{date}", handler.GetOneTransactionNoPartition)

	log.Println("serving in :8080")
	log.Fatal(http.ListenAndServe(":8080", router))
}

type Handler struct {
	Logic *Logic
}

func (h *Handler) PostTransactionPartitioned(w http.ResponseWriter, r *http.Request) {
	type Req struct {
		N      int       `json:"n"`
		UserID string    `json:"user_id"`
		Date   time.Time `json:"date"`
	}

	var reqBody Req
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&reqBody)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var trxs = make([]*Transaction, 0)
	for i := 0; i < reqBody.N; i++ {
		trx := NewLog(i, reqBody.Date)
		trx.UserID = reqBody.UserID

		err = h.Logic.InsertTransactionPartitionedDynamicCached(trx)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		trxs = append(trxs, trx)
	}

	data, _ := json.Marshal(trxs)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
	return
}

func (h *Handler) GetOneTransactionPartitioned(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	userID := chi.URLParam(r, "user_id")
	date, err := time.Parse("2006-01-02", chi.URLParam(r, "date"))
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	trx, err := h.Logic.FetchOneTransactionPartitioned(id, userID, date)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	data, _ := json.Marshal(trx)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
	return
}

func (h *Handler) PostTransactionNoPartition(w http.ResponseWriter, r *http.Request) {
	type Req struct {
		N      int       `json:"n"`
		UserID string    `json:"user_id"`
		Date   time.Time `json:"date"`
	}

	var reqBody Req
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	err := dec.Decode(&reqBody)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	var trxs = make([]*Transaction, 0)
	for i := 0; i < reqBody.N; i++ {
		trx := NewLog(i, reqBody.Date)
		trx.UserID = reqBody.UserID

		err = h.Logic.InsertTransactionWithoutPartition(trx)
		if err != nil {
			w.Write([]byte(err.Error()))
			return
		}

		trxs = append(trxs, trx)
	}

	data, _ := json.Marshal(trxs)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
	return
}

func (h *Handler) GetOneTransactionNoPartition(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	userID := chi.URLParam(r, "user_id")
	date, err := time.Parse("2006-01-02", chi.URLParam(r, "date"))
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	trx, err := h.Logic.FetchOneTransactionWithoutPartition(id, userID, date)
	if err != nil {
		w.Write([]byte(err.Error()))
		return
	}

	data, _ := json.Marshal(trx)
	w.Write(data)
	w.Header().Set("Content-Type", "application/json")
	return
}

type Transaction struct {
	ID           string    `json:"id" db:"id"`
	UserID       string    `json:"user_id" db:"user_id"`
	Info         string    `json:"info" db:"info"`
	Status       string    `json:"status" db:"status"`
	TrxDate      time.Time `json:"trx_date" db:"trx_date"`
	TrxTimestamp time.Time `json:"trx_timestamp" db:"trx_timestamp"` // postgresql only save in millisecond precision, nanosecond will be omitted
}

func NewLog(i int, date time.Time) *Transaction {
	return &Transaction{
		ID:           uuid.NewV4().String(),
		UserID:       fmt.Sprint(i),
		Info:         fmt.Sprintf(`{"iteration": %d}`, i),
		Status:       "SUCCESS",
		TrxDate:      date,
		TrxTimestamp: date,
	}
}

type Logic struct {
	db            *sqlx.DB
	childTableMap sync.Map
}

func NewLogic(db *sqlx.DB) *Logic {
	return &Logic{db: db}
}

func (l *Logic) CreateLogTable() error {
	_, err := l.db.Exec(sqlCreateTableTrx)
	if err != nil {
		err = fmt.Errorf("create transaction table without partition error: %w", err)
		return err
	}

	return nil
}

// InsertTransactionWithoutPartition insert data in logs table without partition
func (l *Logic) InsertTransactionWithoutPartition(trx *Transaction) error {
	sql := `INSERT INTO transactions (id, user_id, info, status, trx_date, trx_timestamp) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *;`
	res, err := l.db.Query(sql, trx.ID, trx.UserID, trx.Info, trx.Status, trx.TrxDate, trx.TrxTimestamp)
	if err != nil {
		return err
	}

	for res.Next() {
		err = res.Scan(&trx.ID, &trx.UserID, &trx.Info, &trx.Status, &trx.TrxDate, &trx.TrxTimestamp)
		if err != nil {
			return err
		}
	}

	return res.Close()
}

// CreatePartitionParentTable create parent table performance.
func (l *Logic) CreatePartitionParentTable() error {
	_, err := l.db.Exec(sqlCreateTableTrxPartitioned)
	if err != nil {
		err = fmt.Errorf("create transaction partitioned parent table error: %w", err)
		return err
	}

	return nil
}

func (l *Logic) ChildTableName(currentTime time.Time) string {
	currentTime = currentTime.UTC()
	currentDay := time.Date(currentTime.Year(), currentTime.Month(), currentTime.Day(), 0, 0, 0, 0, time.UTC)

	year := currentDay.Format("2006")
	month := currentDay.Format("01")
	day := currentDay.Format("02")

	return fmt.Sprintf("transaction_partition_y%s_m%s_d%s", year, month, day)
}

// CreateNewPartitionChildTable will create new partition of logs table if not exist.
func (l *Logic) CreateNewPartitionChildTable(currentTime time.Time) error {
	tableName := l.ChildTableName(currentTime)
	sql := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s PARTITION OF transactions_partitioned FOR VALUES IN ('%s');`,
		tableName,
		currentTime.UTC().Format(time.RFC3339Nano),
	)

	_, err := l.db.Exec(sql)
	if err != nil {
		err = fmt.Errorf("create transaction partitioned child table error: %w: %q", err, sql)
		return err
	}

	return nil
}

// CheckPartitionChildTable return error if not exist, otherwise error is nil
func (l *Logic) CheckPartitionChildTable(tableName string) error {
	sql := fmt.Sprintf("SELECT to_regclass('%s');", tableName)

	var resultTableName string
	res, err := l.db.Query(sql)
	if err != nil {
		err = fmt.Errorf("check table partition error: %w", err)
		return err
	}

	defer func() {
		if _err := res.Close(); _err != nil {
			_err = fmt.Errorf("error close row: %w", _err)
			log.Println(_err)
		}
	}()

	for res.Next() {
		err = res.Scan(&resultTableName)
		if err != nil {
			return err
		}
	}

	if resultTableName != tableName {
		err = fmt.Errorf("not found table %s, returning %s from query", tableName, resultTableName)
		return err
	}

	return nil
}

// InsertTransactionPartitioned insert data into table partition
func (l *Logic) InsertTransactionPartitioned(trx *Transaction) error {
	sql := `INSERT INTO transactions_partitioned (id, user_id, info, status, trx_date, trx_timestamp) VALUES ($1, $2, $3, $4, $5, $6) RETURNING *;`
	res, err := l.db.Query(sql, trx.ID, trx.UserID, trx.Info, trx.Status, trx.TrxDate, trx.TrxTimestamp)
	if err != nil {
		return err
	}

	for res.Next() {
		err = res.Scan(&trx.ID, &trx.UserID, &trx.Info, &trx.Status, &trx.TrxDate, &trx.TrxTimestamp)
		if err != nil {
			return err
		}
	}

	return res.Close()
}

// InsertTransactionPartitionedDynamic insert data into table partition if exist
// Before InsertTransactionPartitioned, always try create partition CreateNewPartitionChildTable,
// because IRL we cannot be sure when to create unless we create cronjob to create next partition
func (l *Logic) InsertTransactionPartitionedDynamic(trx *Transaction) error {
	err := l.CreateNewPartitionChildTable(trx.TrxDate)
	if err != nil {
		return err
	}

	return l.InsertTransactionPartitioned(trx)
}

func (l *Logic) InsertTransactionPartitionedDynamicCached(trx *Transaction) error {
	tableName := l.ChildTableName(trx.TrxDate)
	key := fmt.Sprintf("isExist:%s", tableName)

	// if exist table, then insert data to partition
	exist, ok := l.childTableMap.Load(key)
	if ok && exist != nil {
		if yes, successConversion := exist.(bool); successConversion && yes {
			return l.InsertTransactionPartitioned(trx)
		}
	}

	// if in cache is not exist, try to create then mark it as success
	err := l.CreateNewPartitionChildTable(trx.TrxDate)
	if err != nil {
		return err
	}

	l.childTableMap.Store(key, true)
	return l.InsertTransactionPartitioned(trx)
}

func (l *Logic) FetchOneTransactionWithoutPartition(id, userID string, date time.Time) (*Transaction, error) {
	sql := `SELECT * FROM transactions WHERE id = $1 AND user_id = $2 AND trx_date = $3 ORDER BY trx_timestamp DESC LIMIT 1;`

	var trx = &Transaction{}
	res, err := l.db.Query(sql, id, userID, date)
	if err != nil {
		return trx, err
	}

	defer func() {
		if _err := res.Close(); _err != nil {
			_err = fmt.Errorf("error close row: %w", _err)
			log.Println(_err)
		}
	}()

	for res.Next() {
		err = res.Scan(&trx.ID, &trx.UserID, &trx.Info, &trx.Status, &trx.TrxDate, &trx.TrxTimestamp)
		if err != nil {
			return trx, err
		}
	}

	return trx, nil
}

func (l *Logic) FetchOneTransactionPartitioned(id, userID string, date time.Time) (*Transaction, error) {
	sql := `SELECT * FROM transactions_partitioned WHERE id = $1 AND user_id = $2 AND trx_date = $3 ORDER BY trx_timestamp DESC LIMIT 1;`

	var trx = &Transaction{}
	res, err := l.db.Query(sql, id, userID, date)
	if err != nil {
		return trx, err
	}

	defer func() {
		if _err := res.Close(); _err != nil {
			_err = fmt.Errorf("error close row: %w", _err)
			log.Println(_err)
		}
	}()

	for res.Next() {
		err = res.Scan(&trx.ID, &trx.UserID, &trx.Info, &trx.Status, &trx.TrxDate, &trx.TrxTimestamp)
		if err != nil {
			return trx, err
		}
	}

	return trx, nil
}
