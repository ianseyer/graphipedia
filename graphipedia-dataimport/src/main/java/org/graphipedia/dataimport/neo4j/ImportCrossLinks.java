package org.graphipedia.dataimport.neo4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ImportCrossLinks {

	private final BatchInserter inserter;
	private Map<String, Long> sourceIndex;
	private Map<String,Long> targetIndex;
	private String inputFile;
	private String neo4jdb;
	private String sourceLang;
	private String targetLang;

	public ImportCrossLinks(String inputFile, String neo4jdb, String sourceLang, String targetLang) {
		inserter = BatchInserters.inserter(neo4jdb);
		sourceIndex = new HashMap<String, Long>();
		targetIndex = new HashMap<String, Long>();
		this.inputFile = inputFile;
		this.neo4jdb = neo4jdb;
		this.sourceLang = sourceLang;
		this.targetLang = targetLang;
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			System.out.println("USAGE: ImportCrossLinks <input-file> <neo4jdb> <sourcelang> <targetlang>");
			System.exit(255);
		}
		ImportCrossLinks self = new ImportCrossLinks(args[0], args[1], args[2], args[3]);
		self.importCrossLinks();
	}

	public void importCrossLinks() throws IOException {
		GraphDatabaseService graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jdb);
		registerShutdownHook(graphDb);
		try(Transaction tx = graphDb.beginTx()) {
			System.out.println("Loading nodes in memory...");
			loadNodes(new ExecutionEngine(graphDb));
			tx.success();
		}
		System.out.println("Done!");
		graphDb.shutdown();
		CrossLinkCreator relationshipCreator = new CrossLinkCreator(inserter, sourceIndex, targetIndex);
        long startTime = System.currentTimeMillis();
        relationshipCreator.parse(inputFile);
        long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("\n%d links imported in %d seconds; %d broken links ignored\n",
                relationshipCreator.getLinkCount(), elapsedSeconds, relationshipCreator.getBadLinkCount());
	}

	private void loadNodes(ExecutionEngine engine) {
		String query = "match (n) where n.lang={sourcelang} or n.lang={targetlang} return n.lang,n.wiki-id,n.title,id(n)";
		Map<String, Object> parameters = MapUtil.map("sourcelang", sourceLang, "targetlang", targetLang);
		ExecutionResult result = engine.execute(query, parameters);
		
		for ( Map<String, Object> row : result )
		{
		    Map<String, Object> values = new HashMap<String, Object>();
			for ( Entry<String, Object> column : row.entrySet() )
				values.put(column.getKey(), column.getValue());
		    if( values.get("n.lang").equals(sourceLang) )
		    	sourceIndex.put((String)values.get("n.wiki-id"), (Long)values.get("id(n)"));
		    else 
		    	if (values.get("n.lang").equals(targetLang))
		    		targetIndex.put((String)values.get("n.title"), (Long)values.get("id(n)"));
			
		}
		

	}

	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
	}

}
