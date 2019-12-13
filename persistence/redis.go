package persistence

import (
	"time"

	"github.com/gin-contrib/cache/utils"
	"github.com/go-redis/redis/v7"
)

// RedisStore represents the cache with redis cluster persistence
type RedisStore struct {
	client            redis.UniversalClient
	defaultExpiration time.Duration
}

// ClientOptions proxies Options from the go-redis library
type ClientOptions redis.UniversalOptions

// NewRedisCache returns a RedisStore
func NewRedisCache(opts *ClientOptions, defaultExpiration time.Duration) (*RedisStore, error) {
	uniopts := redis.UniversalOptions(*opts)
	c := redis.NewUniversalClient(&uniopts)

	err := c.Ping().Err()
	if err != nil {
		return nil, err
	}
	return &RedisStore{c, defaultExpiration}, nil
}

// NewRedisCacheFromClient returns a RedisStore from an existing go-redis client
func NewRedisCacheFromClient(client redis.UniversalClient, defaultExpiration time.Duration) *RedisStore {
	return &RedisStore{client, defaultExpiration}
}

// Set (see CacheStore interface)
func (c *RedisStore) Set(key string, value interface{}, expires time.Duration) error {
	value, err := utils.Serialize(value)
	if err != nil {
		return err
	}
	return c.client.Set(key, value, c.expval(expires)).Err()
}

// Add (see CacheStore interface)
func (c *RedisStore) Add(key string, value interface{}, expires time.Duration) error {
	value, err := utils.Serialize(value)
	if err != nil {
		return err
	}
	stored, err := c.client.SetNX(key, value, c.expval(expires)).Result()
	if err != nil {
		return err
	}
	if !stored {
		return ErrNotStored
	}
	return nil
}

// Replace (see CacheStore interface)
func (c *RedisStore) Replace(key string, value interface{}, expires time.Duration) error {
	value, err := utils.Serialize(value)
	if err != nil {
		return err
	}
	if c.client.Exists(key).Val() == 0 {
		return ErrNotStored
	}
	if value == nil {
		return ErrNotStored
	}
	return c.Set(key, value, c.expval(expires))
}

// Get (see CacheStore interface)
func (c *RedisStore) Get(key string, ptrValue interface{}) error {
	val, err := c.client.Get(key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return ErrCacheMiss
		}
		return err
	}
	return utils.Deserialize(val, ptrValue)
}

// Delete (see CacheStore interface)
func (c *RedisStore) Delete(key string) error {
	del, err := c.client.Del(key).Result()
	if err != nil {
		return err
	}
	if del == 0 {
		return ErrCacheMiss
	}
	return nil
}

// Increment (see CacheStore interface)
func (c *RedisStore) Increment(key string, delta uint64) (uint64, error) {
	val, err := c.client.Get(key).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrCacheMiss
		}
		return 0, err
	}
	sum := val + int64(delta)
	err = c.client.Set(key, sum, 0).Err()
	if err != nil {
		return 0, err
	}
	return uint64(sum), nil
}

// Decrement (see CacheStore interface)
func (c *RedisStore) Decrement(key string, delta uint64) (uint64, error) {
	val, err := c.client.Get(key).Int64()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrCacheMiss
		}
		return 0, err
	}
	if delta > uint64(val) {
		delta = uint64(val)
	}
	tempint, err := c.client.DecrBy(key, int64(delta)).Result()
	return uint64(tempint), err
}

// Flush (see CacheStore interface)
func (c *RedisStore) Flush() error {
	return c.client.FlushAll().Err()
}

func (c *RedisStore) expval(expires time.Duration) time.Duration {
	switch expires {
	case DEFAULT:
		return c.defaultExpiration
	case FOREVER:
		return time.Duration(0)
	}
	return expires
}
