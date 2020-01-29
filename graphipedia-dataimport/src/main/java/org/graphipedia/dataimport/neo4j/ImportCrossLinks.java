package org.graphipedia.dataimport.neo4j;

import java.io.IOException;
import java.io.File;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ImportCrossLinks {

	private BatchInserter inserter;
	private String inputFile;
	
	public ImportCrossLinks(String inputFile, String neo4jdb) throws IOException {
		this.inputFile = inputFile;
		inserter = BatchInserters.inserter(new File(neo4jdb));
	}

	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("USAGE: ImportCrossLinks <input-file> <neo4jdb>");
			System.exit(255);
		}
		ImportCrossLinks self = new ImportCrossLinks(args[0], args[1]);
		self.importCrossLinks();
		self.finish();
	}

	public void importCrossLinks() throws IOException {
		CrossLinkCreator relationshipCreator = new CrossLinkCreator(inserter);
		long startTime = System.currentTimeMillis();
		relationshipCreator.parse(inputFile);
		long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
		System.out.printf("\n%d links imported in %d seconds\n",
				relationshipCreator.getLinkCount(), elapsedSeconds);
		inserter.shutdown();
	}

	private void finish() {
		this.inserter.shutdown();
	}
}
