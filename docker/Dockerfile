FROM flink:1.18.0-java8

ENV FLINK_VERSION="1.18.0"
ENV PYTHON_VERSION="3.10.11"
ENV BASE_FILE_PATH="./base_files"
ENV TEMP_WORK_PATH="/tmp/work"
ENV VIRTUAL_ENV=${FLINK_HOME}/venv


# Install dependencies
RUN set -ex \
&& apt update \
&& apt -y install libsnappy1v5 build-essential zlib1g-dev libncurses5-dev libgdbm-dev liblzma-dev libnss3-dev \
libssl-dev libreadline-dev libffi-dev curl libbz2-dev git vim procps telnet zip htop net-tools gdb libunwind-dev \
autoconf graphviz automake libtool sqlite3 libsqlite3-dev node-sqlite3 \
&& rm -rf /var/lib/apt/lists/*


# Install python
RUN mkdir -p ${TEMP_WORK_PATH}

RUN cd ${TEMP_WORK_PATH} \
&& wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tar.xz \
&& tar xvf Python-${PYTHON_VERSION}.tar.xz

RUN cd ${TEMP_WORK_PATH}/Python-${PYTHON_VERSION} \
&& ./configure --enable-optimizations \
&& make -j 8 \
&& make altinstall \
&& ln -snf /usr/local/bin/python3.10 /usr/bin/python \
&& ln -snf /usr/local/bin/python3.10 /usr/bin/python3 \
&& ln -snf /usr/local/bin/pip3.10 /usr/bin/pip \
&& ln -snf /usr/local/bin/pip3.10 /usr/bin/pip3



# RUN cd ${TEMP_WORK_PATH} \
# && mkdir -p /opt/flink/jemalloc/libunwind/src/
# && git clone https://github.com/libunwind/libunwind.git \
# && cd libunwind \
# && ./configure CC=icc CFLAGS="-g -O3 -ip" CXX=icc CCAS=gcc CCASFLAGS=-g LDFLAGS="-L$PWD/src/.libs" \
# && autoconf -i

# && make \
# && make install

# COPY ${BASE_FILE_PATH}/src /opt/flink/jemalloc/libunwind/src


RUN cd ${TEMP_WORK_PATH} \
&& mkdir -p /opt/flink/jemalloc/memory-ag \
&& mkdir -p /opt/flink/jemalloc/prof/jeprof 
# \
# && git clone https://github.com/jemalloc/jemalloc.git \
# && cd jemalloc \
# && git checkout master \
# && autoconf \
# && ./configure --enable-prof --enable-stats --enable-debug --enable-fill --enable-prof-libunwind --prefix=/opt/flink/jemalloc/memory-ag/ \
# # --with-static-libunwind=/opt/flink/jemalloc/libunwind/src/.libs/libunwind-x86_64.a
# && make \
# && make install \
# && chmod 777 -R /opt/flink/jemalloc/*


# Install dependencies of python
COPY ${BASE_FILE_PATH}/openjdk/include ${JAVA_HOME}/include
COPY ${BASE_FILE_PATH}/requirements.txt ${TEMP_WORK_PATH}
RUN python -m pip install --upgrade pip && pip install -r ${TEMP_WORK_PATH}/requirements.txt


# Flink
#COPY ${BASE_FILE_PATH}/lib/*.jar ${FLINK_HOME}/lib/

RUN mv ${FLINK_HOME}/examples/* ${FLINK_HOME}/examples/java | mkdir ${FLINK_HOME}/examples/java

RUN cd ${TEMP_WORK_PATH} \
&& wget https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
&& wget https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/1.18.1/flink-parquet-1.18.1.jar \
&& wget https://repo1.maven.org/maven2/com/github/oshi/oshi-core/6.4.12/oshi-core-6.4.12.jar \
&& wget https://repo1.maven.org/maven2/org/apache/flink/flink-table-api-java-uber/1.18.1/flink-table-api-java-uber-1.18.1.jar \
&& wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink/0.14.1/hudi-flink-0.14.1.jar \
&& wget https://repo1.maven.org/maven2/org/apache/parquet/parquet-avro/1.13.1/parquet-avro-1.13.1.jar \
&& wget https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.13.1/parquet-hadoop-1.13.1.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/hudi/hudi-flink1.17-bundle/0.14.0/hudi-flink1.17-bundle-0.14.0.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-bundle/0.7.0-incubating/paimon-bundle-0.7.0-incubating.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-flink-1.18/0.7.0-incubating/paimon-flink-1.18-0.7.0-incubating.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/0.7.0-incubating/paimon-oss-0.7.0-incubating.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-hive-connector-2.1-cdh-6.3/0.7.0-incubating/paimon-hive-connector-2.1-cdh-6.3-0.7.0-incubating.jar \
&& wget https://www.apache.org/dyn/closer.lua/flink/flink-table-store-0.3.0/flink-table-store-hive-catalog-0.3.0_2.3.jar \
&& wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.1.0-1.18/flink-sql-connector-kafka-3.1.0-1.18.jar \
&& cp ./*.jar ${FLINK_HOME}/lib \
&& wget https://archive.apache.org/dist/flink/flink-${FLINK_VERSION}/python/apache-flink-${FLINK_VERSION}.tar.gz \
&& tar -zxvf apache-flink-${FLINK_VERSION}.tar.gz \
&& rm -rf ${FLINK_HOME}/examples/python \
&& mv ${TEMP_WORK_PATH}/apache-flink-${FLINK_VERSION}/deps/examples/* ${FLINK_HOME}/examples/python | mkdir ${FLINK_HOME}/examples/python
RUN chown -R flink:flink ${FLINK_HOME}

#RUN ln -s 'prof_prefix:/opt/flink/prof,prof:true,lg_prof_interval:30,lg_prof_sample:19,prof_leak:true' /etc/malloc.conf
# RUN ln -s 'prof_prefix:/opt/flink/prof,prof:true,lg_prof_interval:30,lg_prof_sample:19,prof_final:true,prof_leak:true' /etc/malloc.conf
USER flink

RUN mkdir ${FLINK_HOME}/test
RUN cd ${FLINK_HOME}/test && ln -sfn ${FLINK_HOME}/src src

RUN cd ${FLINK_HOME} \
&& python -m venv ${VIRTUAL_ENV} \
&& chmod +x "${VIRTUAL_ENV}/bin/activate" \
&& echo ". ${VIRTUAL_ENV}/bin/activate" >> .bashrc \
&& echo "alias ll='ls -al'" >> .bashrc \
&& echo "export HADOOP_USER_NAME=impala" >> .bashrc
# && echo "export LD_PRELOAD='/usr/lib/x86_64-linux-gnu/libjemalloc.so.2'" >> .bashrc \
# && echo "export PATH='/opt/flink/jemalloc/memory-ag/bin:${PATH}'" >> .bashrc \
# && echo "export MALLOC_ARENA_MAX=4" >> .bashrc \
# && echo "export MALLOC_CONF='prof_prefix:/opt/flink/prof,prof:true,lg_prof_interval:30,lg_prof_sample:19,prof_final:true,prof_leak:true'" >> .bashrc


RUN . ${VIRTUAL_ENV}/bin/activate \
&& python -m pip install --upgrade pip && pip install -r ${TEMP_WORK_PATH}/requirements.txt

WORKDIR ${FLINK_HOME}

# docker build -t flink:1.18.0 .
# flink run --python ~/src/main.py --pyFile ~/test/
# jeprof --show_bytes --add_lib=/opt/flink/jemalloc/libunwind/src --gif ${JAVA_HOME}/bin/java ~/prof*.heap > ~/src/result.gif