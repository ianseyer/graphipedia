package org.graphipedia.dataimport.neo4j;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import org.graphipedia.dataimport.ProgressCounter;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class CrossLinkCreator {
	
	private BatchInserter inserter;
	private Map<String, Long> sourceIndex;
	private Map<String, Long> targetIndex;
	
	private final ProgressCounter linkCounter = new ProgressCounter();
	private int badLinkCount = 0;
	
	public CrossLinkCreator(BatchInserter inserter,  Map<String, Long> sourceIndex, Map<String, Long> targetIndex) {
		this.inserter = inserter;
		this.sourceIndex = sourceIndex;
		this.targetIndex = targetIndex;
	}
	
	public int getLinkCount() {
        return linkCounter.getCount();
    }

    public int getBadLinkCount() {
        return badLinkCount;
    }
    
    private void createRelationship(Long sourceNodeId, Long targetNodeId) {
        
    	if( sourceNodeId != null && targetNodeId != null ) {
    		inserter.createRelationship(sourceNodeId, targetNodeId, WikiRelationship.Crosslink, null);
    		linkCounter.increment();
    	}
    	else
    		badLinkCount++;
        
    }
    
    public void parse(String inputFile) throws IOException {
    	BufferedReader bd = new BufferedReader(new FileReader(inputFile));
    	String line;
    	while( (line=bd.readLine())!= null ) {
    		String[] nodes = line.split("\t");
    		createRelationship(sourceNodeId( nodes[0]), targetNodeId(nodes[1]));
    	}
    	bd.close();
    }
    
    private Long sourceNodeId(String wikiId) {
    	return sourceIndex.get(wikiId);
    }
    
    private Long targetNodeId(String title) {
    	return targetIndex.get(title);
    }

}
