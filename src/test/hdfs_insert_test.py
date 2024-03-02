
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaConsumer, StreamingFileSink
from pyflink.common import WatermarkStrategy, Configuration
from pyflink.datastream.state_backend import EmbeddedRocksDBStateBackend

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col, concat
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

from pyflink.table import StreamTableEnvironment, DataTypes

# def kafka_to_hdfs():
#     config = Configuration()
#     config.set_string("python.executable", "/opt/flink/venv/bin/python3")
#     env = StreamExecutionEnvironment.get_execution_environment()
    

#     settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
#     env = StreamTableEnvironment.create(env, environment_settings=settings)

        
    
    
            
#     env.create_temporary_table(
#     'sink',
#     TableDescriptor.for_connector('filesystem')
#         .schema(Schema.new_builder()
#                 .column('word', DataTypes.STRING())
#                 .column('count', DataTypes.BIGINT())
#                 .build())
#         .option('path', output_path)
#         .format(FormatDescriptor.for_format('canal-json')
#                 .build())
#         .build())


    # source_ddl = """
    #     CREATE TABLE kafka_source (
    #         text STRING
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'topic' = 'iis_log',
    #         'properties.bootstrap.servers' = 'kafka:9092',
    #         'properties.group.id' = 'testGroup',
    #         'format' = 'json',
    #         'scan.startup.mode' = 'earliest-offset'
    #     )
    # # """
    # env.execute_sql(source_ddl)
    
    # 'scan.startup.mode' = 'latest-offset'
    
    # sink_ddl = """
    #     CREATE TABLE hdfs_sink (
    #         text STRING
    #     ) WITH (
    #         'connector' = 'filesystem',
    #         'path' = 'hdfs://namenode:8020/user/hive/warehouse/managed/crm.db/actors',
    #         'format' = 'parquet'
    #     )

        
        
    # sink_ddl = """
    #     CREATE TABLE hdfs_sink (
    #         text STRING
    #     ) WITH (
    #         'connector' = 'kafka',
    #         'properties.bootstrap.servers' = 'kafka:9092',
    #         'topic' = 'target',
    #         'format' = 'json'
    #     )
    # """ 
    # env.execute_sql(sink_ddl)
    # env.from_path('kafka_source') \
    # .select(concat(col('text'),'234234234234234234')) \
    # .execute_insert('hdfs_sink').wait()
    
#Flink의 Table API 연산에 문자열이나 Python 객체를 직접 전달하려고 할 때 Java API가 예상하는 Java 객체가 아닌 Python 객체를 받게 되어 발생하는 문제입니다.    

# {"text":"hahahha"}
# hadoop fs -ls hdfs://namenode:8020/user/hive/warehouse/managed/crm.db/actors
# hdfs dfs -getfacl hdfs://namenode:8020/user/hive/warehouse/managed/crm.db
# hdfs dfs -setfacl -R -m user:flink:rwx hdfs://namenode:8020/user/hive/warehouse/managed/crm.db/actors

    
def kafka_to_hdfs():
    # 실행 환경 설정
    config = Configuration()
    config.set_string("python.executable", "/opt/flink/venv/bin/python3")
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    env = StreamTableEnvironment.create(env, environment_settings=settings)
    
    source_ddl = """
        CREATE TABLE kafka_source (
            text STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'iis_log',
            'properties.bootstrap.servers' = 'kafka:9092',
            'properties.group.id' = 'testGroup',
            'format' = 'json',
            'scan.startup.mode' = 'earliest-offset'
        )
    """
    env.execute_sql(source_ddl)
    
    sink_ddl = """
        CREATE TABLE hdfs_sink (
            text STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'hdfs://namenode:8020/user/hive/warehouse/external/crm.db/actors2',
            'format' = 'parquet'
        )
    """
    env.execute_sql(sink_ddl)
    env.from_path("kafka_source") \
        .select(col('text')) \
        .execute_insert('hdfs_sink').wait()
    
if __name__ == "__main__":
    kafka_to_hdfs()