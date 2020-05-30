package storage

import (
	"context"
	"github.com/codding-buddha/ds-pb/storage"
	"github.com/codding-buddha/ds-pb/utils"
	guuid "github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func testDbCreate(t *testing.T) (*storage.KeyValueStore, func()) {
	logger := utils.NewConsole(false, "client")
	dbName, _ := guuid.NewUUID()
	db, err := storage.New(logger, dbName.String())

	if err != nil {
		t.Fatalf("Failed to create db with name %s", dbName.String())
	}

	return db, func() {
		os.Remove(dbName.String())
	}
}
func find(records []storage.Record, val storage.Record) (int, bool) {
	for i, item := range records {
		if item.Key == val.Key && item.Value == val.Value {
			return i, true
		}
	}
	return -1, false
}

func TestGetAll(t *testing.T) {
	db, cleanup := testDbCreate(t)
	defer cleanup()
	cases := []struct {
		key, value string
	}{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}

	for _, c := range cases {
		db.Write(context.Background(), storage.Record{
			Key:   c.key,
			Value: c.value,
		})
	}
	records, err := db.GetAll()
	assert.Empty(t, err)
	for index, record := range records {
		assert.Equal(t, cases[index].key, record.Key)
		assert.Equal(t, cases[index].value, record.Value)
	}
}

func TestBulkUpdate(t *testing.T) {
	db, cleanup := testDbCreate(t)
	defer cleanup()
	bulkUpdateRecords := []storage.Record{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
	}

	db.BulkUpdate(context.Background(), &bulkUpdateRecords)
	records, err := db.GetAll(context.Background())
	assert.Empty(t, err)
	assert.Equal(t, len(bulkUpdateRecords), len(records))
	for _, record := range records {
		_, ok := find(bulkUpdateRecords, record)
		assert.True(t, ok)
	}
}