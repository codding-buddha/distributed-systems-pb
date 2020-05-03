package storage

import (
	"github.com/codding-buddha/ds-pb/utils"
	"github.com/juju/errors"
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
func (lookup *KeyValueStore) Get(key string) (Record, error) {
	var r Record
	lookup.logger.Debug().Str("key", key).Msg("get request")
	err := lookup.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(KvBucket))
		v := b.Get([]byte(key))

		if v == nil {
			lookup.logger.Info().Str("key", key).Msg("Non-existent key")
			return errors.NotFoundf("Key %S not found ", key)
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
func (lookup *KeyValueStore) Write(record Record) error {
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

// Close cleanups underlying open connection if any
func (lookup *KeyValueStore) Close() {
	if lookup.db != nil {
		err := lookup.db.Close()
		if err != nil {
			lookup.logger.Error().Err(err).Msg("DB connection close errored out.")
		}
	}
}
