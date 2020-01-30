#!/bin/bash
java -classpath ./graphipedia-dataimport/target/graphipedia-dataimport.jar org.graphipedia.dataimport.ExtractLinks enwiki.xml links-out.xml

java -Xmx12G -classpath ./graphipedia-dataimport/target/graphipedia-dataimport.jar org.graphipedia.dataimport.neo4j.ImportGraph links-out.xml enwiki.db
