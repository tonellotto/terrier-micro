package it.cnr.isti.hpclab.matching.structures.query;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.util.Util;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Paths;

import static com.google.common.base.Preconditions.checkArgument;

import org.terrier.indexing.tokenisation.IdentityTokeniser;
import org.terrier.indexing.tokenisation.Tokeniser;

/** 
 * This class reads queries, one per line, verbatim from the specified file. 
 * 
 * By default, queries are tokenised by this class,
 * and are passed verbatim to the query parser. Tokenisation can be turned off by the property
 * <tt>micro.query.tokenise</tt>, with the tokensier specified by <tt>tokeniser</tt>.
 * 
 * Moreover, this class assumes that the first token on each line is the query Id. This can be controlled
 * by the properties <tt>micro.query.id</tt> (default false). Trailing colons in the query Id
 * are removed (aka TREC single line format from the Million Query track).
 * *  
 * <p><b>Properties:</b>
 * <ul>
 * 		<li><tt>micro.queries</tt> a comma separated list of filenames, containing the queries</li>
 * 		<li><tt>micro.queries.id</tt> - does the line start with a query id? (defaults to false) </li>
 * 		<li><tt>micro.queries.tokenise</tt> (defaults to false). By default, the query is not passed through a tokeniser. If set to true, then it will be passed through the tokeniser configured by the <tt>tokeniser</tt> property.</li>
 * 		<li><tt>micro.queries.num</tt> how many queries to process</li>
 * </ul>
 */

public class TextQuerySource implements QuerySource
{
	protected String[] mQueries;  /** The queries in the query file. */
	protected int[]    mQueryIds; /** The query identifiers in the query file. */
	
	protected int mQueryIndex = 0; /** Pointer to current read position in query array. */
	
	public TextQuerySource() 
	{
		this(MatchingConfiguration.get(Property.QUERY_FILE));		
	}
	
	public TextQuerySource(final File queryfile)
	{
		this(queryfile.getName());
	}
	
	public TextQuerySource(final String queryfilename)
	{
		try {
			checkArgument(queryfilename != null && queryfilename.length() > 0);

			int num_queries = Integer.MAX_VALUE;
			if (!MatchingConfiguration.get(Property.NUM_QUERIES).equals("all"))
				num_queries = Integer.parseInt(MatchingConfiguration.get(Property.NUM_QUERIES));

			extractQueries(queryfilename, num_queries);			
		} catch (Exception ioe) {
			LOGGER.error("Problem getting the " + queryfilename + " file:", ioe);
			return;
		}
	}

	private boolean extractQueries(final String queryFilename, final int num_queries)
	{		
		LOGGER.info("Extracting queries from " + queryFilename + " - queryids: "+ MatchingConfiguration.get(Property.HAS_QUERY_ID));

		ObjectList<String> queries = new ObjectArrayList<String>();
		IntList 		   qids    = new IntArrayList();

		if (!Files.exists(Paths.get(queryFilename))) {
			LOGGER.error("The topics file " + queryFilename + " does not exist, or it cannot be read.");
			return false;
		}

		boolean gotSome = false;
		Tokeniser tokeniser = MatchingConfiguration.getBoolean(Property.MUST_TOKENISE) ? Tokeniser.getTokeniser() : new IdentityTokeniser();

		String line = null;
		int queryCount = 0;

		try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(queryFilename)))) {
			while ((line = br.readLine()) != null) {
				line = line.trim();
				
				queryCount++;
				int qid;
				String query;
				if (MatchingConfiguration.getBoolean(Property.HAS_QUERY_ID)) {
					final int qidEnd = Util.IndexOfAny(line, new char[]{' ', '\t', ':'});

					if (qidEnd == -1)
						continue;

					qid   = Integer.parseInt(line.substring(0,qidEnd));
					query = line.substring(qidEnd + 1);
				} else {
					query = line;
					qid = queryCount;
				}
				
				query = Util.JoinStrings(tokeniser.getTokens(new StringReader(query)), " ");
				
				queries.add(query);
				qids.add(qid);
				gotSome = true;
				LOGGER.debug("Extracted queryID " + qid + "  "+ query);
				
				if (queries.size() == num_queries)
					break;
			}
		} catch (IOException ioe) {
			LOGGER.error("IOException while extracting queries: ",ioe);	
			return gotSome;
		}
		LOGGER.info("Extracted "+ queries.size() + " queries");
		
		this.mQueries = queries.toArray(new String[0]);
		this.mQueryIds = qids.toArray(new int[0]);

		return gotSome;
	}

	@Override
	public boolean hasNext() 
	{
		return mQueryIndex != mQueries.length;
	}

	@Override
	public String next() 
	{
		if (mQueryIndex == mQueries.length)
			return null;
		return mQueries[mQueryIndex++];
	}

	public int getQueryId()
	{
		if (mQueryIndex == 0)
			throw new UnsupportedOperationException();
		
		return mQueryIds[mQueryIndex - 1];
	}

	public void reset()
	{
		mQueryIndex = 0;
	}		
}