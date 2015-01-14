Graphipedia
===========

A tool for creating a graph database of Wikipedia pages and the links between them.

What's new
--------------

The tool has been modified from the base version to:

1. Extract and import the Wikipedia categories and the links between them (in addition to the nodes and
the links).

2. Add a property "lang" to each node, which specifies the ISO 639-1 code of the Wikipedia language 
from which the node has been extracted.

3. Add cross-language links between nodes that represent equivalent pages in two different languages (e.g. the page titled "Paris" in the English Wikipedia and the page titled "Parigi" in the Italian Wikipedia).


Importing Data
--------------

The graphipedia-dataimport module allows to create a [Neo4j](http://neo4j.org)
database from a Wikipedia database dump.

See [Wikipedia:Database_download](http://en.wikipedia.org/wiki/Wikipedia:Database_download)
for instructions on getting a Wikipedia database dump.

Assuming you downloaded `pages-articles.xml.bz2`, follow these steps:

1.  Run ExtractLinks to create a smaller intermediate XML file containing page titles
    and links only. The best way to do this is decompress the bzip2 file and pipe the output directly to ExtractLinks:

    `bzip2 -dc pages-articles.xml.bz2 | java -classpath graphipedia-dataimport.jar org.graphipedia.dataimport.ExtractLinks - enwiki-links.xml en`

2.  Run ImportGraph to create a Neo4j database with nodes and relationships into
    a `graph.db` directory. The following arguments "`en false 12 150`" describe the
    language code, whether to append the graph to an existing database, the estimated
    number of nodes and edges in millions. A good estimate is crucial, or neo4j will
    not be able to keep everything in memory!

    `java -Xmx8G -classpath graphipedia-dataimport.jar org.graphipedia.dataimport.neo4j.ImportGraph enwiki-links.xml graph.db en false 12 150`

Just to give an idea, `enwiki-20141106-pages-articles.xml.bz2` is 11.4G and
contains about 12M pages, resulting in over 150M links to be extracted.

On my laptop _with an SSD drive_ the import takes about 30 minutes to decompress/ExtractLinks (pretty much the same time
as decompressing only) and an additional 10 minutes to ImportGraph.

(Note that disk I/O is the critical factor here: the same import will easily take several hours with an old 5400RPM drive.)

Querying
--------

The [Neo4j browser](http://blog.neo4j.org/2013/10/neo4j-200-m06-introducing-neo4js-browser.html) can be used to query and visualise
the imported graph. Here are some sample Cypher queries.

Show all pages linked to a given starting page - e.g. "Neo4j":

    MATCH (p0:Page {title:'Neo4j'}) -[Link]- (p:Page)
    RETURN p0, p

Find how two pages - e.g. "Neo4j" and "Kevin Bacon" - are connected:

    MATCH (p0:Page {title:'Neo4j'}), (p1:Page {title:'Kevin Bacon'}),
      p = shortestPath((p0)-[*..6]-(p1))
    RETURN p
