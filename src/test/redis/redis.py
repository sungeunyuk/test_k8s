from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy, Configuration
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
# 환경 설정
from pyflink.table.descriptors import Schema
import os
from pyflink.table.table_descriptor import TableDescriptor
# HDFS 사용자 이름 설정
os.environ["HADOOP_USER_NAME"] = "impala"
os.environ["USER"] = "impala"
from pyflink.table import EnvironmentSettings, TableEnvironment

from pyflink.datastream.connectors.kafka import (
    KafkaSource,
    KafkaOffsetsInitializer,
    KafkaOffsetResetStrategy,
)
from pyflink.common.serialization import SimpleStringSchema

#from pyflink.table import StreamTableEnvironment, DataTypes
import redis

# Kafka 설정
kafka_topic = 'source'
kafka_bootstrap_servers = 'kafka:9092'  # Kafka 서버 주소

# Redis 설정
redis_host = 'redis'
redis_port = 6379
redis_db = 0

def redis_sink(value):
    # Redis 클라이언트 생성
    r = redis.Redis(host=redis_host, port=redis_port, db=redis_db)
    # Redis에 데이터 저장, 여기서는 간단히 키를 값과 동일하게 설정
    r.set(value[0], value)

    
def kafka_to_hdfs():
    # 실행 환경 설정
    config = Configuration()
    config.set_string("python.executable", "/opt/flink/venv/bin/python3")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    #env.set_stream_time_characteristic(TimeCharacteristic.ProcessingTime)
    env.set_state_backend(EmbeddedRocksDBStateBackend())

    
    source = (
            KafkaSource.builder()
            .set_bootstrap_servers("kafka:9092")
            .set_topics('source')
            .set_group_id('self._consumer_group_id')
            .set_starting_offsets(KafkaOffsetsInitializer.committed_offsets(
                KafkaOffsetResetStrategy.LATEST))
            .set_value_only_deserializer(SimpleStringSchema())
            .build()
        )
    
    stream = env.from_source(
            source=source,
            watermark_strategy=WatermarkStrategy.no_watermarks(),
            source_name='self._source_topic',
        ).set_parallelism(2)
    
    stream.key_by(
                lambda x: x[0],
                key_type=Types.STRING(),
            ).map(redis_sink)
    
    # 실행
    env.execute("PyFlink Kafka to Redis")
    
if __name__ == "__main__":
    kafka_to_hdfs()