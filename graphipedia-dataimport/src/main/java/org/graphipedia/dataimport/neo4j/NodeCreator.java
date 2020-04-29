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

import java.util.Arrays;
import java.util.Map;

import org.graphipedia.dataimport.ProgressCounter;
import org.graphipedia.dataimport.SimpleStaxParser;
import org.graphipedia.dataimport.WikipediaNamespace;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.unsafe.batchinsert.BatchInserter;

public class NodeCreator extends SimpleStaxParser {

    private final BatchInserter inserter;
    private final Map<String, Long> inMemoryIndex;
    private final String langCode;
    
    private String title;
    private String wikiId;
    private String text;
    private final ProgressCounter pageCounter = new ProgressCounter();
    private int numberOfCategories;
    private int numberOfPages; 

    public NodeCreator(BatchInserter inserter, Map<String, Long> inMemoryIndex, String langCode) {
        super(Arrays.asList("p", "t", "full_text", "i"));
        this.inserter = inserter;
        this.inMemoryIndex = inMemoryIndex;
        this.langCode = langCode;
        this.numberOfCategories = 0;
        this.numberOfPages = 0;
        title = null;
        wikiId = null;
        text = null;
    }

    public int getPageCount() {
        return pageCounter.getCount();
    }
    
    public int getNumberOfPages() {
    	return numberOfPages;
    }
    
    public int getNumberOfCategories () {
    	return numberOfCategories;
    }

    @Override
    protected void handleElement(String element, String value) {
        if ("p".equals(element)) {
            createNode(title, text, wikiId);
            title = null;
            wikiId = null;
            text = null;
        } else if ("t".equals(element)) {
        	title = value;
        } else if ("i".equals(element)) {
        	wikiId = value;
        } else if ("full_text".equals(element)) {
            text = value;
        }
        	
    }

    private void createNode(String title, String text, String wikiId) {
        Map<String, Object> properties = MapUtil.map(WikiNodeProperty.wikiid.name(), wikiId, WikiNodeProperty.text.name(), text, WikiNodeProperty.title.name(), title, WikiNodeProperty.lang.name(), langCode);
        boolean isCategory = title.startsWith(WikipediaNamespace.getCategoryName(langCode)+":");
        WikiLabel label = null;
        if (isCategory) {
        	numberOfCategories += 1;
        	label = WikiLabel.Category;
        }
        else {
        	numberOfPages += 1;
        	label = WikiLabel.Page;
        }
        System.out.println(properties);
        System.out.println(label);
        long nodeId = inserter.createNode(properties, label);
        inMemoryIndex.put(title, nodeId);
        pageCounter.increment();
    }

}
