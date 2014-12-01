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
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ImportCrossLinks {

	private BatchInserter inserter;
	private Map<String, Long> sourceIndex;
	private Map<String,Long> targetIndex;
	private String inputFile;
	private String neo4jdb;
	private String sourceLang;
	private String targetLang;

	public ImportCrossLinks(String inputFile, String neo4jdb, String sourceLang, String targetLang) {
		
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
		//registerShutdownHook(graphDb);
		try(Transaction tx = graphDb.beginTx()) {
			System.out.println("Loading nodes in memory...");
			loadNodes(new ExecutionEngine(graphDb));
			tx.success();
		}
		System.out.println("Done!");
		graphDb.shutdown();
		inserter = BatchInserters.inserter(neo4jdb);
		CrossLinkCreator relationshipCreator = new CrossLinkCreator(inserter, sourceIndex, targetIndex);
		long startTime = System.currentTimeMillis();
		relationshipCreator.parse(inputFile);
		long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
		System.out.printf("\n%d links imported in %d seconds; %d broken links ignored\n",
				relationshipCreator.getLinkCount(), elapsedSeconds, relationshipCreator.getBadLinkCount());
		inserter.shutdown();
	}

	private void loadNodes(ExecutionEngine engine) {
		String query1 = "match (n:Page {lang:\"" + sourceLang + "\"}) return n.`wiki-id` as identifier,id(n)";
		String query2 = "match (n:Category {lang:\"" + sourceLang + "\"}) return n.`wiki-id` as identifier,id(n)";
		String query3 = "match (n:Page {lang:\"" + targetLang + "\"}) return n.title as identifier,id(n)";
		String query4 = "match (n:Category {lang:\"" + targetLang + "\"}) return n.title as identifier,id(n)";
		executeQuery(engine, query1, sourceIndex);
		executeQuery(engine, query2, sourceIndex);
		executeQuery(engine, query3, targetIndex);
		executeQuery(engine, query4, targetIndex);

	}

	private void executeQuery(ExecutionEngine engine, String query, Map<String, Long> index) {
		ExecutionResult result = engine.execute(query);

		for ( Map<String, Object> row : result )
		{
			Map<String, Object> values = new HashMap<String, Object>();
			for ( Entry<String, Object> column : row.entrySet() )
				values.put(column.getKey(), column.getValue());
			index.put((String)values.get("identifier"), (Long)values.get("id(n)"));

		}
	}

	

}
