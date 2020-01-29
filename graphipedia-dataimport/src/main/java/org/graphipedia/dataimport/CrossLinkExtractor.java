package org.graphipedia.dataimport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.io.InputStreamReader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.graphipedia.dataimport.neo4j.WikiLabel;
import org.graphipedia.dataimport.neo4j.WikiNodeProperty;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class CrossLinkExtractor {

	private static final Pattern LINKS_PATTERN = Pattern.compile("INSERT INTO `langlinks` VALUES (.+)");

	private static final Pattern LINK_PATTERN = Pattern.compile("\\((.+?),'(.*?)','(.*?)'\\)");

	private BufferedWriter[] bw;
	private String sourceLang;
	private String[] targetLangs;
	private GraphDatabaseService graphDb;
	private final ProgressCounter linkCounter = new ProgressCounter();

	public CrossLinkExtractor(BufferedWriter[] bw, String sourceLang, String[] targetLangs, String neo4jdb) {
		this.bw = bw;
		this.sourceLang = sourceLang;
		this.targetLangs = targetLangs;
		this.graphDb = new GraphDatabaseFactory().newEmbeddedDatabase(new File(neo4jdb));
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
						targetPage = targetPage.substring(0, 1).toUpperCase() + targetPage.substring(1);
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
		ResourceIterator<Node> pages = graphDb.findNodes(WikiLabel.Page, WikiNodeProperty.wikiid.name(), wikiId);
		if(pages.hasNext())
			return pages.next().getId();
		
		ResourceIterator<Node> categories = graphDb.findNodes(WikiLabel.Category, WikiNodeProperty.wikiid.name(), wikiId);
		if (categories.hasNext())
			return categories.next().getId();
		
		return -1;
	}

	private long getNodeIdByTitle(String title, String lang) { 
		ResourceIterator<Node> pages = graphDb.findNodes(WikiLabel.Page, WikiNodeProperty.title.name(), title);
		if(pages.hasNext())
			return pages.next().getId();
		
		ResourceIterator<Node> categories = graphDb.findNodes(WikiLabel.Category, WikiNodeProperty.title.name(), title);
		if (categories.hasNext())
			return categories.next().getId();
		
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
