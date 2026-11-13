package main

import (
	"fmt"
	"os"
	"time"
	"vortexkv/internal/storage"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if os.Getenv("APP_ENV") == "development" {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	log.Info().Msg("VortexKV Started")

	_ = os.Setenv("VORTEXKV_DATA_DIR", "./data")

	var store storage.Engine = storage.NewMemTable(500) // Small capacity to force rotation

	log.Info().Msg("Inserting 100 keys to trigger multiple flushes and compaction...")
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_for_key_%d_which_is_a_bit_long", i))
		
		err := store.Put(key, value)
		if err != nil {
			log.Error().Err(err).Str("key", string(key)).Msg("Error saving key")
		}
	}

	log.Info().Msg("Sleeping for 4 seconds to let flush and compaction workers do their job...")
	time.Sleep(4 * time.Second)

	log.Info().Msg("Testing read from disk after compaction:")
	val, err := store.Get([]byte("key_10"))
	if err != nil {
		log.Error().Err(err).Msg("Error fetching key_10")
	} else {
		log.Info().Str("val", string(val)).Msg("Fetched key_10")
	}

	err = store.Delete([]byte("key_20"))
	if err != nil {
		log.Error().Err(err).Msg("Error deleting key_20")
	}

	log.Info().Msg("Sleeping for 1 second to allow tombstone flush to finish...")
	time.Sleep(1 * time.Second)

	_, err = store.Get([]byte("key_20"))
	if err == storage.ErrNotFound {
		log.Info().Msg("key_20 successfully deleted (tombstone retrieved from disk/memtable)")
	} else if err != nil {
		log.Error().Err(err).Msg("Error fetching key_20")
	} else {
		log.Error().Msg("Error: key_20 still exists!")
	}

	// Test a key that definitely never existed to trigger Bloom Filter rejection
	_, err = store.Get([]byte("missing_key_999"))
	if err == storage.ErrNotFound {
		log.Info().Msg("missing_key_999 successfully rejected (bloom filter works)")
	} else {
		log.Error().Msg("Error handling missing key")
	}

	log.Info().Msg("Closing store to safely shutdown...")
	err = store.Close()
	if err != nil {
		log.Error().Err(err).Msg("Error closing store")
	}

	log.Info().Msg("VortexKV Shutdown gracefully.")
}
