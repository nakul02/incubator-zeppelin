# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM maven:3.3.3-jdk-8
RUN apt-get update && apt-get install -y git curl wget unzip bzip2
# RUN curl -sL https://deb.nodesource.com/setup_5.x | bash -  && apt-get install -y nodejs &&\
#RUN git clone https://github.com/apache/incubator-systemml &&\
#    cd incubator-systemml && mvn package -P distribution &&\
#    mvn install:install-file -Dfile=target/systemml-0.10.0-incubating-SNAPSHOT-standalone.jar -DgroupId=org.apache.systemml -DartifactId=systemml -Dversion=0.10.0-incubating -Dpackaging=jar &&\
#    cd .. && git clone -b spark_dml https://github.com/nakul02/incubator-zeppelin.git &&\
#    cd incubator-zeppelin && mvn package -DskipTests

RUN git clone https://github.com/nakul02/incubator-systemml               &&\
    cd incubator-systemml                                                 &&\
    mvn package -P distribution -DskipTests                               &&\
    mvn install -DskipTests                                               &&\
    cd ..                                                                 &&\
    git clone -b spark_dml https://github.com/nakul02/incubator-zeppelin  &&\
    cd incubator-zeppelin                                                 &&\
    mvn clean package -Pbuild-distr -DskipTests                           &&\
    mv zeppelin-distribution/target/zeppelin-0.6.0-SNAPSHOT.tar.gz / &&\
    cd /                                                                  &&\
    rm -rf incubator-systemml incubator-zeppelin                          &&\
    tar xvf zeppelin-0.6.0-SNAPSHOT.tar.gz

ADD http://snap.stanford.edu/data/amazon0601.txt.gz /
CMD ["/zeppelin-0.6.0-SNAPSHOT/bin/zeppelin.sh"]
EXPOSE 8080 8081
