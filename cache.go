package idgenerator

import (
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

//CachedIDGenerator 缓存ID生成器实体
type CachedIDGenerator struct {
	//configEntity redis配置
	configEntity *RedisConfig
	redisPool    *redis.Pool
	baseOption   *Option
	mapContainer map[string]*idContainer
}

// RedisConfig Redis配置
type RedisConfig struct {
	UserName  string
	Password  string
	URL       string
	MaxIdle   int
	MaxActive int
}

// idContainer ID容器
type idContainer struct {
	lowID      int64
	highID     int64
	expiration time.Time
	option     *Option
	lock       sync.RWMutex // 加锁
}

// Option Option
type Option struct {
	IncrNum int64 //获取的ID数量
	Seconds int   //获取到的ID过期时间(s)
	Model   int   // 模式 默认1 扩展用
}

//BuildCacheEntity 创建CacheEntity
func BuildCacheEntity(config *RedisConfig) *CachedIDGenerator {
	cacheEntity := CachedIDGenerator{}
	cacheEntity.configEntity = config
	cacheEntity.redisPool = redisPollInit(&cacheEntity)
	cacheEntity.baseOption = defaultOption()
	cacheEntity.mapContainer = make(map[string]*idContainer)
	return &cacheEntity
}

//SetBaseOption 配置全局参数
func (cache *CachedIDGenerator) SetBaseOption(option *Option) error {
	err := checkOption(option)
	if err != nil {
		return err
	}
	cache.baseOption = option
	return nil
}

// SetOption 针对IDKey配置参数
func (cache *CachedIDGenerator) SetOption(IDKey string, option *Option) error {
	err := checkOption(option)
	if err != nil {
		return err
	}
	oldContainer := cache.mapContainer[IDKey]
	if oldContainer != nil {
		oldContainer.option = option
		return nil
	}
	newContainer := idContainer{}
	newContainer.option = option
	cache.mapContainer[IDKey] = &newContainer
	return nil
}

// GetIDByKey 获取分布式ID
func (cache *CachedIDGenerator) GetIDByKey(IDKey string) (int64, error) {
	mapContainer := cache.mapContainer
	container := mapContainer[IDKey]
	if container == nil {
		container = &idContainer{}
		container.option = cache.baseOption
		cache.mapContainer[IDKey] = container
	}
	result, err := cache.getID(IDKey)
	if err == errNotInit ||
		err == errUseUp ||
		err == errExpiration {
		err = cache.fetchIDs(IDKey)
		if err != nil {
			return 0, err
		}
		return cache.getID(IDKey)
	} else if err != nil {
		return 0, err
	}
	return result, nil
}

// INCRBY counter 30
func (cache *CachedIDGenerator) fetchIDs(IDKey string) error {
	mapContainer := cache.mapContainer
	container := mapContainer[IDKey]
	conn := cache.redisPool.Get()
	defer conn.Close()
	valueGet, err := redis.Int64(conn.Do("INCRBY", IDKey, container.option.IncrNum))
	if err != nil {
		return err
	}
	container.lowID = valueGet - container.option.IncrNum + 1
	container.highID = valueGet
	if container.option.Seconds > 0 {
		duration, err := time.ParseDuration(strconv.Itoa(container.option.Seconds) + "s")
		if err != nil {
			return err
		}
		container.expiration = time.Now().Add(duration)
	}
	return nil
}

// redisPollInit 初始化Redis线程池
func redisPollInit(cache *CachedIDGenerator) *redis.Pool {
	configEntity := cache.configEntity
	return &redis.Pool{
		MaxIdle:   configEntity.MaxIdle,
		MaxActive: configEntity.MaxActive,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", configEntity.URL)
			if err != nil {
				return nil, err
			}
			_, err = c.Do(configEntity.UserName, configEntity.Password)
			if err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
	}
}

func defaultOption() *Option {
	baseOption := Option{}
	baseOption.IncrNum = 100
	baseOption.Seconds = 10
	baseOption.Model = 1
	return &baseOption
}

// GetID 获取分布式ID
func (cache *CachedIDGenerator) getID(IDkey string) (int64, error) {
	container := cache.mapContainer[IDkey]
	container.lock.Lock()
	defer container.lock.Unlock()
	if container.lowID == 0 && container.highID == 0 {
		return 0, errNotInit
	}
	if container.option.Seconds == 0 ||
		(container.option.Seconds > 0 && time.Now().Before(container.expiration)) {
		if container.lowID < container.highID {
			id := container.lowID
			container.lowID = container.lowID + 1
			return id, nil
		} else if container.lowID == container.highID {
			id := container.lowID
			return id, nil
		} else {
			return 0, errUseUp
		}
	}
	return 0, errExpiration
}

// checkOption 检查option
func checkOption(option *Option) error {
	if option.IncrNum < 1 {
		return errors.New("IncrNum必须大于或等于1")
	}
	if option.Model != 1 {
		return errors.New("Model 必须为1")
	}
	if option.Seconds < 0 {
		return errors.New("Seconds 必须大于等于0")
	}
	return nil
}

var (
	errNotInit    = errors.New("还没有初始化")
	errUseUp      = errors.New("本地ID池耗尽")
	errExpiration = errors.New("本地数据过期")
)
