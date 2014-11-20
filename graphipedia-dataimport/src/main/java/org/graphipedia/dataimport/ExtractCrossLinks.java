package org.graphipedia.dataimport;

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;

public class ExtractCrossLinks {
	
	private static final String STDIN_FILENAME = "-";

	public static void main(String[] args) throws IOException {
		if (args.length < 3) {
			System.out.println("USAGE: ExtractCrossLinks <input-file> source-lang target-lang1#target-lang2...");
			System.exit(255);
		}
		ExtractCrossLinks self = new ExtractCrossLinks();
		self.extract(args[0], args[1], args[2].split("#"));
	}
	
	public void extract(String inputFile, String sourceLang, String[] targetLangs) throws IOException {
		BufferedWriter[] bw = new BufferedWriter[targetLangs.length];
		for( int i = 0; i < bw.length; i += 1 ) {
			bw[i] = new BufferedWriter(new FileWriter(sourceLang + "-" + targetLangs[i] + ".txt"));
		}
		long startTime = System.currentTimeMillis();
		CrossLinkExtractor crossLinkExtractor = new CrossLinkExtractor(bw, targetLangs);
		if (STDIN_FILENAME.equals(inputFile)) {
			crossLinkExtractor.parse(System.in);
        } else {
            crossLinkExtractor.parse(new FileInputStream(inputFile));
        }
		
		for( int i = 0; i < bw.length; i += 1 ) {
			bw[i].close();
		}
		
		long elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000;
        System.out.printf("\n%d links parsed in %d seconds.\n", crossLinkExtractor.getLinkCount(), elapsedSeconds);
	}

}
