package storage

import (
	"context"
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
	"github.com/opentracing/opentracing-go"
	bolt "go.etcd.io/bbolt"
	"time"
)

const KvBucket = "KVStore"

// Simple key-value store built using boltdb.
type KeyValueStore struct {
	db     *bolt.DB
	logger *utils.Logger
}

// Represents record in key-value store
type Record struct {
	Key   string
	Value string
}

//New creates new KeyValueStore
func New(logger *utils.Logger, dbname string) (*KeyValueStore, error) {
	var l = KeyValueStore{}
	db, err := bolt.Open(dbname, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err == nil {
		err = db.Update(func(tx *bolt.Tx) error {
			b, err := tx.CreateBucketIfNotExists([]byte(KvBucket))
			if err != nil {
				return errors.Annotatef(err, "db setup failure: seeding bucket %S failed", KvBucket)
			}

			logger.Printf("bucket initialization : success, name : %v, key-count : %v", KvBucket, b.Stats().KeyN)
			return nil
		})
	}

	if err != nil {
		return &l, errors.Annotate(err, "db init failed")
	}

	l.db = db
	l.logger = logger
	return &l, nil
}

//Get fetches record for given key from the store
func (lookup *KeyValueStore) Get(ctx context.Context, key string) (Record, error) {
	var r Record
	span, _ :=	opentracing.StartSpanFromContext(ctx, "db.get")
	defer span.Finish()
	span.SetTag("db.operation", "get(key:" + key + ")")
	lookup.logger.Debug().Str("key", key).Msg("get request")
	err := lookup.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KvBucket))
		v := b.Get([]byte(key))

		if v == nil {
			lookup.logger.Info().Str("key", key).Msg("Non-existent key")
			return errors.NotFoundf("Key %s ", key)
		}

		r = Record{
			Key:   key,
			Value: string(v[:]),
		}

		return nil
	})
	return r, err
}

// Write create/update record in store
func (lookup *KeyValueStore) Write(ctx context.Context, record Record) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "db.write")
	defer span.Finish()
	span.SetTag("db.operation", "update(key:" + record.Key + " value:" + record.Value + ")")
	lookup.logger.Debug().Str("key", record.Key).Str("value", record.Value).Msg("write request")
	err := lookup.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KvBucket))
		if b == nil {
			return errors.New("bucket not found\n")
		}
		err := b.Put([]byte(record.Key), []byte(record.Value))
		return err
	})

	return err
}

func (lookup *KeyValueStore) Delete(key string) error {
	lookup.logger.Printf("Deleting record with key : %s", key)
	err := lookup.db.Update(func (tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KvBucket))
		if b == nil {
			return errors.New("bucket not found\n")
		}
		err := b.Delete([]byte(key))
		return err
	})

	return err
}

func (lookup *KeyValueStore) GetAll(ctx context.Context) ([]Record, error) {
	span, _ :=	opentracing.StartSpanFromContext(ctx, "db.getAll")
	defer span.Finish()
	lookup.logger.Printf("Fetch all keys from db")
	var records []Record
	err := lookup.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(KvBucket))
		if b == nil {
			return errors.New("bucket not found\n")
		}
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			records = append(records, Record{
				Key:   string(k[:]),
				Value: string(v[:]),
			})
		}

		return nil
	})
	lookup.logger.Printf("Fetch all complete total records : %v", len(records))

	return records, err
}

func (lookup *KeyValueStore) BulkUpdate(ctx context.Context, records *[]Record) error {
	span, _ := opentracing.StartSpanFromContext(ctx, "db.bulkUpdate")
	defer span.Finish()
	// Start a writable transaction.
	tx, err := lookup.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Use the transaction...
	b := tx.Bucket([]byte(KvBucket))
	if b == nil {
		return errors.New("bucket not found\n")
	}

	for _, record := range *records {
		err = b.Put([]byte(record.Key), []byte(record.Value))
		if err != nil {
			break
		}
	}

	if err != nil {
		return err
	}

	// Commit the transaction and check for error.
	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}


// Close cleanups underlying open connection if any
func (lookup *KeyValueStore) Close() {
	if lookup.db != nil {
		err := lookup.db.Close()
		if err != nil {
			lookup.logger.Error().Err(err).Msg("DB connection close errored out.")
		}
	}
}
