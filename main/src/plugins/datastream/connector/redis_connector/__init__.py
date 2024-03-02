import redis

# Redis 설정
redis_host = 'redis'
redis_port = 6379
redis_db = 0

def redis_sink(value):
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    r.set(value[0], value)
    
    return value