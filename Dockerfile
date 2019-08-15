FROM maven:3.6-jdk-12

ADD . .

RUN mvn package

CMD ["java", "-classpath", "./graphipedia-dataimport/target/graphipedia-dataimport.jar", "org.graphipedia.dataimport.ExtractLinks", "towiki.xml", "links-out.xml"]
