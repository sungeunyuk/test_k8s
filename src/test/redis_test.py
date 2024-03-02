import redis

redis_host = 'redis'
redis_port = 6379
redis_db = 0


def redis_sink(value):
    # Redis 클라이언트 생성
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    # Redis에 데이터 저장, 여기서는 간단히 키를 값과 동일하게 설정
    r.set(value, value)
 
def redis_get(value):
    # Redis 클라이언트 생성
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    # Redis에 데이터 저장, 여기서는 간단히 키를 값과 동일하게 설정
    print(r.get(value))
       

redis_sink('hahah')
redis_get('hahah')

