import jpype
import jpype.imports
from jpype.types import *
 
import time
 
def main(username):
    # JVM 시작 + 필요한 JAR 불러오기
    jpype.startJVM(classpath=[
        'jars/hadoop-client-api-3.0.0.jar',
        'jars/hadoop-client-runtime-3.0.0.jar',
        'jars/hadoop-client-minicluster-3.0.0.jar',
 
        'jars/commons-logging-1.2.jar',
        'jars/slf4j-api-2.0.7.jar',
        'jars/junit-4.13.2.jar',
 
        'jars/apache-log4j-extras-1.2.17.jar',
        'jars/htrace-core4-4.2.0-incubating.jar',
    ])
 
    # Java Class 가져오기
    from java.lang import System
    from org.apache.hadoop.hdfs import MiniDFSCluster
    from org.apache.hadoop.conf import Configuration
 
    # HDFS 프록시 유저 권한 설정
    hdfs_config = Configuration()
    hdfs_config.set(f"hadoop.proxyuser.${username}.hosts", "*")
    hdfs_config.set(f"hadoop.proxyuser.${username}.groups", "*")
    hdfs_config.set("dfs.permissions", "true")
 
    # HDFS 테스트를 위한 임시 디렉터리
    System.setProperty('test.build.data', '/tmp/hdfs-tmp/')
 
    # HDFS 시작
    minicluster = MiniDFSCluster.Builder(hdfs_config).nameNodePort(32955).nameNodeHttpPort(42501).numDataNodes(1).format(True).build()
 
    # time.sleep(300)
 
    minicluster.shutdown()
 
if __name__ == "__main__":
    username = "mark"
    main(username)