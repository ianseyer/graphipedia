package org.graphipedia.dataimport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CrossLinkExtractor {

	private static final Pattern LINKS_PATTERN = Pattern.compile("INSERT INTO `langlinks` VALUES (.+)");

	private static final Pattern LINK_PATTERN = Pattern.compile("\\((.+?),'(.*?)','(.*?)'\\)");

	private BufferedWriter[] bw;
	private String sourceLang;
	private String[] targetLangs;
	private GraphDatabaseService graphDb;
	private ExecutionEngine engine;
	private final ProgressCounter linkCounter = new ProgressCounter();

	public CrossLinkExtractor(BufferedWriter[] bw, String sourceLang, String[] targetLangs, String neo4jdb) {
		this.bw = bw;
		this.sourceLang = sourceLang;
		this.targetLangs = targetLangs;
		this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(neo4jdb);
		this.engine = new ExecutionEngine(graphDb);
		registerShutdownHook(graphDb);
	}

	public int getLinkCount() {
		return linkCounter.getCount();
	}

	public void parse(InputStream inputFile) throws IOException {
		try(Transaction tx = graphDb.beginTx()) {
			BufferedReader bd = new BufferedReader(new InputStreamReader(inputFile, "UTF-8"));
			String line = "";
			while( (line = bd.readLine()) != null ) {
				Matcher matcher = LINKS_PATTERN.matcher(line);
				if ( matcher.find() ) {
					String links = matcher.group(1);
					Matcher matcher1 = LINK_PATTERN.matcher(links);
					while(matcher1.find()) {
						String sourcePage = matcher1.group(1);
						String targetLang = matcher1.group(2);
						String targetPage = matcher1.group(3);
						int fileIndex;
						if( (fileIndex = isTargetLang(targetLang)) != -1 && targetPage.length() > 0 && 
								( !targetPage.contains(":") || targetPage.startsWith(WikipediaNamespace.getCategoryName(targetLang)+":") ) ) {
							long sourcePageId = getNodeIdByWikiId(sourcePage, sourceLang);
							long targetPageId = getNodeIdByTitle(targetPage, targetLangs[fileIndex]);
							if( sourcePageId != -1 && targetPageId != -1 ) {
								bw[fileIndex].write(sourcePageId + "\t" + targetPageId + "\n");
								linkCounter.increment();
							}
						}

					}

				}
			}
			tx.success();
			bd.close();
		}
	}

	public void terminate() {
		graphDb.shutdown();
	}

	private long getNodeIdByWikiId(String wikiId, String lang) {
		String query1 = "match (n:Page {`wiki-id`:\""+ wikiId +"\"}) where n.lang=\""+lang+"\" return id(n)";
		ExecutionResult result = engine.execute(query1);
		Iterator<Long> it = result.columnAs("id(n)");
		if( it.hasNext() )
			return it.next();
		String query2 = "match (n:Category {`wiki-id`:\""+ wikiId +"\"}) where n.lang=\""+lang+"\" return id(n)";
		result = engine.execute(query2);
		it = result.columnAs("id(n)");
		if( it.hasNext() )
			return it.next();

		return -1;
	}

	private long getNodeIdByTitle(String title, String lang) { 
		String query1 = "match (n:Page {title:\""+ title +"\"}) where n.lang=\""+lang+"\" return id(n)";
		ExecutionResult result = engine.execute(query1);
		Iterator<Long> it = result.columnAs("id(n)");
		if( it.hasNext() )
			return it.next();
		String query2 = "match (n:Category {title:\""+ title +"\"}) where n.lang=\""+lang+"\" return id(n)";
		result = engine.execute(query2);
		it = result.columnAs("id(n)");
		if( it.hasNext() )
			return it.next();

		return -1;
	}

	private int isTargetLang(String targetLang) {
		for( int i = 0; i < targetLangs.length; i += 1 )
			if( targetLangs[i].equals(targetLang) )
				return i;

		return -1;
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
