FROM maven:3.6-jdk-12

ADD . .

RUN mvn install

CMD ["java", "-classpath", "./graphipedia-dataimport/target/graphipedia-dataimport.jar", "org.graphipedia.dataimport.ExtractLinks", "towiki.xml", "/work/links-out.xml"]
