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
package org.graphipedia.dataimport;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;

public class LinkExtractor extends SimpleStaxParser {

    private static final Pattern LINK_PATTERN = Pattern.compile("\\[\\[(.+?)\\]\\]");

    private final XMLStreamWriter writer;
    private final ProgressCounter pageCounter = new ProgressCounter();

    private String title;
    private String text;
    private String id;
    private boolean extractId;

    public LinkExtractor(XMLStreamWriter writer) {
        super(Arrays.asList("page", "title", "text", "id"));
        this.writer = writer;
        this.extractId = true;
    }

    public int getPageCount() {
        return pageCounter.getCount();
    }

    @Override
    protected void handleElement(String element, String value) {
        if ("page".equals(element)) {
            if (!title.contains(":") || title.startsWith("Category:")) {
                try {
                    writePage(title, text);
                } catch (XMLStreamException streamException) {
                    throw new RuntimeException(streamException);
                }
            }
            title = null;
            text = null;
            id = null;
            extractId = true;
        } else if ("title".equals(element)) {
            title = value;
        } else if ("text".equals(element)) {
            text = value;
        } else if ("id".equals(element)) {
        	if (extractId) {
        		id = value;
        		extractId = false;
        	}
        }
    }

    private void writePage(String title, String text) throws XMLStreamException {
        writer.writeStartElement("p");
        
        writer.writeStartElement("t");
        writer.writeCharacters(title);
        writer.writeEndElement();
        
        Set<String> links = parseLinks(text);
        links.remove(title);
        
        for (String link : links) {
            writer.writeStartElement("l");
            writer.writeCharacters(link);
            writer.writeEndElement();
        }
        
        writer.writeEndElement();

        pageCounter.increment();
    }

    private Set<String> parseLinks(String text) {
        Set<String> links = new HashSet<String>();
        if (text != null) {
            Matcher matcher = LINK_PATTERN.matcher(text);
            while (matcher.find()) {
                String link = matcher.group(1);
                if (!link.contains(":") || link.startsWith("Category:")) {
                    if (link.contains("|")) {
                        link = link.substring(link.lastIndexOf('|') + 1);
                    }
                    links.add(link);
                }
            }
        }
        return links;
    }

}
