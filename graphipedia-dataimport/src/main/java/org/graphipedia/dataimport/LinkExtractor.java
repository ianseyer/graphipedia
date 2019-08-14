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

    public LinkExtractor(XMLStreamWriter writer) {
        super(Arrays.asList("page", "title", "text"));
        this.writer = writer;
    }

    public int getPageCount() {
        return pageCounter.getCount();
    }

    @Override
    protected void handleElement(String element, String value) {
        // System.out.print("element ");
        // System.out.println(element);
        // System.out.print("value ");        
        // System.out.println(value);

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
        } else if ("title".equals(element)) {
            title = value;
        } else if ("text".equals(element)) {
            text = value;
            if (value.toString().endsWith("]]")) {
                // pattern matching from https://stackoverflow.com/a/11255490
                Matcher m = Pattern.compile(
                            Pattern.quote("[[")
                            + "(.*?)"
                            + Pattern.quote("]]")
                   ).matcher(value);
                while(m.find()){
                    String match = m.group(1);
                    System.out.println(">"+match+"<");
                    //here you insert 'match' into the list
                }
                // List<String> strings = Arrays.asList( input.replaceAll("^.*?[[", "").split("]].*?([[|$)"));
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
//                System.out.println(strings);
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
                System.out.println("CATEGORY");
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

}
