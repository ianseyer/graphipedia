//
// Copyright (c) 2012 Mirko Nasato
//
// Permission is hereby granted, free of charge, to any person obtaining a
// copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
package org.graphipedia.dataimport.neo4j;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ImportGraph {

	private final BatchInserter inserter;
	private final Map<String, Long> inMemoryIndex;

	public ImportGraph(String dataDir, boolean append) {
		inserter = BatchInserters.inserter(dataDir);
		if( !append ) {
			inserter.createDeferredSchemaIndex(WikiLabel.Page).on("title").create();
			inserter.createDeferredSchemaIndex(WikiLabel.Page).on("lang").create();
			inserter.createDeferredSchemaIndex(WikiLabel.Category).on("title").create();
			inserter.createDeferredSchemaIndex(WikiLabel.Category).on("lang").create();
		}
		inMemoryIndex = new HashMap<String, Long>();
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("USAGE: ImportGraph <input-file> <data-dir> <lang-code> <append>");
			System.exit(255);
		}
		String inputFile = args[0];
		String dataDir = args[1];
		String langCode = args[2];
		boolean append = Boolean.parseBoolean(args[3]);
		ImportGraph importer = new ImportGraph(dataDir, append);
		importer.createNodes(inputFile, langCode);
		importer.createRelationships(inputFile);
		importer.finish();
	}

	public void createNodes(String fileName, String langCode) throws Exception {
		System.out.println("Importing pages...");
		NodeCreator nodeCreator = new NodeCreator(inserter, inMemoryIndex, langCode);
		long startTime = System.currentTimeMillis();
		nodeCreator.parse(fileName);
		long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
		System.out.printf("\n%d pages and %d categories imported in %d seconds.\n", nodeCreator.getNumberOfPages(), 
				nodeCreator.getNumberOfCategories(), elapsedSeconds);
	}

	public void createRelationships(String fileName) throws Exception {
		System.out.println("Importing links...");
		RelationshipCreator relationshipCreator = new RelationshipCreator(inserter, inMemoryIndex);
		long startTime = System.currentTimeMillis();
		relationshipCreator.parse(fileName);
		long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
		System.out.printf("\n%d links imported in %d seconds; %d broken links ignored\n",
				relationshipCreator.getLinkCount(), elapsedSeconds, relationshipCreator.getBadLinkCount());
	}

	public void finish() {
		inserter.shutdown();
	}

}
