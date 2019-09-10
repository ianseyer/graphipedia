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
import java.util.ArrayList;
import java.util.List;
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

    public LinkExtractor(XMLStreamWriter writer) {
        super(Arrays.asList("page", "title", "text", "id"));
        this.writer = writer;
    }

    public int getPageCount() {
        return pageCounter.getCount();
    }


    @Override
    protected void createNode(String title, String text, ArrayList<String> categories, String id) {
        String nothing;
    }

    @Override
    protected void handleElement(String element, String value) {
        if ("page".equals(element)) {
            if (!title.contains(":")) {
                try {
                    writePage(title, text);
                } catch (XMLStreamException streamException) {
                    throw new RuntimeException(streamException);
                }
            }
            title = null;
            text = null;
            id = null;
        } else if ("title".equals(element)) {
            title = value;
        } else if ("text".equals(element)) {
            text = value;
        } else if ("id".equals(element)) {
            id = value;
        }
    }

    private void writePage(String title, String text) throws XMLStreamException {
        writer.writeStartElement("page");

        writer.writeStartElement("title");
        writer.writeCharacters(title);
        writer.writeEndElement();

        writer.writeStartElement("id");
        writer.writeCharacters(id);
        writer.writeEndElement();

        Set<String> links = parseLinks(text);
        links.remove(title);

        for (String link : links) {
            writer.writeStartElement("link");
            writer.writeCharacters(link);
            writer.writeEndElement();
        }

        Set<String> categories = parseCategories(text);
        for (String category : categories) {
            writer.writeStartElement("category");
            writer.writeCharacters(category);
            writer.writeEndElement();
        }

        writer.writeStartElement("text");
        writer.writeCharacters(text);
        writer.writeEndElement();

        writer.writeEndElement();

        pageCounter.increment();
    }

    private Set<String> parseLinks(String text) {
        Set<String> links = new HashSet<String>();
        if (text != null) {
            Matcher matcher = LINK_PATTERN.matcher(text);
            while (matcher.find()) {
                String link = matcher.group(1);
                if (!link.contains(":")) {
                    if (link.contains("|")) {
                        link = link.substring(0, link.lastIndexOf('|'));
                    }
                    links.add(link);
                }
            }
        }
        return links;
    }

    private Set<String> parseCategories(String text) {
        Set<String> categories = new HashSet<String>();
        if (text.toString().endsWith("]]")) {
            if (text != null) {
                Matcher matcher = Pattern.compile(
                            Pattern.quote("[[Category:")
                            + "(.*?)"
                            + Pattern.quote("]]")
                    ).matcher(text);
                while(matcher.find()){
                    String category = matcher.group(1);
                    categories.add(category);
                }
            }
        }
        return categories;
    }

}
