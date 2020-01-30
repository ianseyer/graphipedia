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
import java.io.File;
import java.io.IOException;

import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class ImportGraph {

	private final BatchInserter inserter;
	private final Map<String, Long> inMemoryIndex;

	public ImportGraph(String dataDir, boolean append, int mNodes, int mEdges) throws IOException {
		inserter = BatchInserters.inserter(new File(dataDir), generateConfig(mNodes, mEdges));
		if( !append ) {
			inserter.createDeferredSchemaIndex(WikiLabel.Page).on(WikiNodeProperty.title.name()).create();
			inserter.createDeferredSchemaIndex(WikiLabel.Page).on(WikiNodeProperty.lang.name()).create();
			inserter.createDeferredSchemaIndex(WikiLabel.Page).on(WikiNodeProperty.wikiid.name()).create();
			inserter.createDeferredSchemaIndex(WikiLabel.Category).on(WikiNodeProperty.title.name()).create();
			inserter.createDeferredSchemaIndex(WikiLabel.Category).on(WikiNodeProperty.lang.name()).create();
			inserter.createDeferredSchemaIndex(WikiLabel.Category).on(WikiNodeProperty.wikiid.name()).create();
		}
		inMemoryIndex = new HashMap<String, Long>();
	}

	/**
	 * Generates a configuration suitable for batch imports. Reserves enough space to fit nodes and
	 * relationships into memory. Estimates based on
	 * http://neo4j.com/docs/stable/configuration-io-examples.html#configuration-batchinsert
	 * @param mNodes expected number of nodes to import (in millions)
	 * @param mEdges expected number of edges to import (in millions)
	 * @return a neo4j configuration that provides enough memory for an import of the specified size
	 */
	protected Map<String, String> generateConfig(int mNodes, int mEdges) {
	  Map<String, String> config = new HashMap<>();
	  int required = (mNodes * 9) + (mEdges * 33);
	  String nodes = (mNodes * 9) + "M";
	  String edges = (mEdges * 33) + "M";
    config.put( "neostore.nodestore.db.mapped_memory", nodes );
    config.put( "neostore.relationshipstore.db.mapped_memory", edges );
    config.put( "eostore.propertystore.db.mapped_memory", "50M" );
    config.put( "neostore.propertystore.db.strings.mapped_memory", "100M" );
    config.put( "neostore.propertystore.db.arrays.mapped_memory", "0M" );

    System.out.printf("To keep everything in memory, neo4j will require a total of %s for nodes "
        + "and %s for edges.\n", nodes, edges);
    long xmx = Runtime.getRuntime().maxMemory() / 1048576;
    System.out.printf("The JVM has %sM available, which is%s enough to keep everything in memory.\n",
        xmx, xmx > required ? "" : " not");

	  return config;
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 4) {
			System.out.println("USAGE: ImportGraph <input-file> <data-dir> <lang-code> (<append> <million-nodes> <milltion-edges>)");
			System.exit(255);
		}
		String inputFile = args[0];
		String dataDir = args[1];
		String langCode = args[2];
		boolean append = args.length > 3 ? Boolean.parseBoolean(args[3]) : false;
		int mNodes = args.length > 4 ? Integer.parseInt(args[4]) : 10;
		int mEdges = args.length > 5 ? Integer.parseInt(args[5]) : 100;
		ImportGraph importer = new ImportGraph(dataDir, append, mNodes, mEdges);
		importer.createNodes(inputFile, langCode);
		importer.createRelationships(inputFile);
		importer.finish();
	}

	public void createNodes(String fileName, String langCode) throws Exception {
		System.out.println("Importing pages...");
		NodeCreator nodeCreator = new NodeCreator(inserter, inMemoryIndex, langCode);
		long startTime = System.currentTimeMillis();
        System.out.println(fileName);
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
