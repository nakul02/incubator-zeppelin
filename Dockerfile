FROM maven:3.3.3-jdk-8
RUN apt-get update && apt-get install -y git curl wget unzip bzip2
# RUN curl -sL https://deb.nodesource.com/setup_5.x | bash -  && apt-get install -y nodejs &&\
#RUN git clone https://github.com/apache/incubator-systemml &&\
#    cd incubator-systemml && mvn package -P distribution &&\
#    mvn install:install-file -Dfile=target/systemml-0.10.0-incubating-SNAPSHOT-standalone.jar -DgroupId=org.apache.systemml -DartifactId=systemml -Dversion=0.10.0-incubating -Dpackaging=jar &&\
#    cd .. && git clone -b spark_dml https://github.com/nakul02/incubator-zeppelin.git &&\
#    cd incubator-zeppelin && mvn package -DskipTests

RUN git clone -b zeppelin https://github.com/apache/incubator-systemml    &&\
    cd incubator-systemml                                                 &&\
    mvn package -P distribution                                           &&\
    mvn install                                                           &&\
    cd ..                                                                 &&\
    git clone -b spark_dml https://github.com/nakul02/incubator-zeppelin  &&\
    cd incubator-zeppelin                                                 &&\
    mvn package -DskipTests

ADD http://snap.stanford.edu/data/amazon0601.txt.gz /
CMD ["/incubator-zeppelin/bin/zeppelin.sh"]
EXPOSE 8080 8081
