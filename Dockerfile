FROM maven:3.6-jdk-12

ARG wikiname

WORKDIR /etl

ADD . .

RUN mvn install

RUN java -classpath ./graphipedia-dataimport/target/graphipedia-dataimport.jar org.graphipedia.dataimport.ExtractLinks ${wikiname}.xml links-out.xml

CMD ["java", "-Xmx12G", "-classpath", "./graphipedia-dataimport/target/graphipedia-dataimport.jar", "org.graphipedia.dataimport.neo4j.ImportGraph", "links-out.xml", "${wikiname}}.db"]
