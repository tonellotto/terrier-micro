package it.cnr.isti.hpclab.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URL;

import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import org.terrier.indexing.tokenisation.Tokeniser;
import org.terrier.indexing.tokenisation.UTFTokeniser;

import org.terrier.terms.PorterStemmer;
import org.terrier.terms.Stopwords;
import org.terrier.utility.Files;

/**
 * This class parses a stream of queries doing stopword removal and stemming.
 * If enabled, terms not appearing in a given index are removed.
 * 
 * TODO: Implement lexicon filtering
 * 
 * @author Nicola Tonellotto
 *
 */
public class Queries 
{
	static {
		URL url = Queries.class.getResource("/terrier.properties");
		System.setProperty("terrier.setup", url.getFile().toString());
	}
	
	static {
	
		// The following does not work because of the new FileSystem stuff in org.terrier.utility
		// URL url = Queries.class.getResource("/stopword-list.txt");
		// Setup.setProperty("stopwords.filename", url.toString());
		// The following hack creates a temporary file with the stopword-list.txt contents copied in
		File file = null;
		try {
			InputStream input = Queries.class.getResourceAsStream("/stopword-list.txt");
			file = File.createTempFile("tempfile", ".tmp");
			OutputStream out = new FileOutputStream(file);
			
			int read;
			byte[] bytes = new byte[1024];
			while ((read = input.read(bytes)) != -1) {
				out.write(bytes, 0, read);
			}
			out.close();
			
			System.setProperty("stopwords.filename", file.toString());
			file.deleteOnExit();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}

	public static final class Args 
	{
		// required arguments

		@Option(name = "-input", metaVar = "[String]", required = true, usage = "Input file containing one query per line")
		public String input;

		// optional arguments

		@Option(name = "-stem", metaVar = "[boolean]", required = false, usage = "Applies stemming through class org.terrier.terms.PorterStemmer")
		public boolean stem;
		
		@Option(name = "-stop", metaVar = "[boolean]", required = false, usage = "Applies stopword removal through class org.terrier.terms.Stopwords")
		public boolean stop;

	}

	private static Tokeniser tokeniser = new UTFTokeniser();
	
	public static void main(String argv[]) throws IOException 
	{
		// parser.addArgument("-l", "--lexicon").help("Applies lexicon filtering").action(Arguments.storeTrue());
		
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(90));
		try {
			parser.parseArgument(argv);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}

        
        String inputFile   = args.input;
        
        boolean stemmingFlag = args.stem;
        boolean stopwordFlag = args.stop;
        // boolean lexiconFlag  = ns.getBoolean("lexicon");
                
        // URL url = Queries.class.getResource("/stopword-list.txt");
        
        // Stopwords stopword = new Stopwords(null, url.toString());
        Stopwords stopword = new Stopwords(null);
		PorterStemmer stemmer  = new PorterStemmer(null);
/*        
		Index index = null;
		Lexicon<String> lex = null;
		if (lexiconFlag) {
			Index.setIndexLoadingProfileAsRetrieval(false);
			index = Index.createIndex();
			if (Index.getLastIndexLoadError() != null) {
				System.err.println(Index.getLastIndexLoadError());
				System.exit(-2);
			}
			lex = index.getLexicon();
		}
*/		
        BufferedReader br = Files.openFileReader(inputFile);
        
        String line, tokens[];
		StringBuffer finalQuery = null;
        
		while ((line = br.readLine()) != null) {
			finalQuery = new StringBuffer();
									
			tokens = tokeniser.getTokens(new StringReader(line));
			for (int i = 0; i < tokens.length; ++i) {
				String term = tokens[i].trim();
				if (term.length() > 0)
					if (stopwordFlag) {
						if (!stopword.isStopword(term)) {
							if (stemmingFlag)
								term = stemmer.stem(term);
						} else {
							term = "";
						}
					} else {
						if (stemmingFlag)
							term = stemmer.stem(term);
					}

					if (term.length() > 0)
						/*
						if (lexiconFlag) {
							LexiconEntry le = lex.getLexiconEntry(term);
							if (le != null) {
								finalQuery.append(term + " ");
							}
						} else
						*/
							finalQuery.append(term + " ");
				}
			if (finalQuery.length() > 0)
				System.out.println(finalQuery.toString().trim());
		}
        /*
		if (lex != null)
			lex.close();
		if (index != null)
			index.close();
        */
		br.close();
        System.exit(0);
	}
}