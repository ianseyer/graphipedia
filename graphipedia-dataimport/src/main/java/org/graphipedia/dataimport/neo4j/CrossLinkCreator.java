package org.graphipedia.dataimport.neo4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.graphipedia.dataimport.ProgressCounter;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class CrossLinkCreator {

	private BatchInserter inserter;

	private final ProgressCounter linkCounter = new ProgressCounter();
	
	public CrossLinkCreator(BatchInserter inserter) {
		this.inserter = inserter;
	}

	public int getLinkCount() {
		return linkCounter.getCount();
	}

	public void parse(String inputFile) throws IOException {
		BufferedReader bd = new BufferedReader(new FileReader(inputFile));
		String line;
		while( (line=bd.readLine())!= null ) {
			String[] nodes = line.split("\t");
			long firstNode = Long.parseLong(nodes[0]);
			long secondNode = Long.parseLong(nodes[1]);
			inserter.createRelationship(firstNode, secondNode, WikiRelationship.Crosslink, null);
			linkCounter.increment();
		}
		bd.close();
	}
}
