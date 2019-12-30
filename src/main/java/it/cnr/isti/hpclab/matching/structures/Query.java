/*
 * Micro query processing framework for Terrier 5
 *
 * Copyright (C) 2018-2019 Nicola Tonellotto 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */

package it.cnr.isti.hpclab.matching.structures;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.query.SimpleQueryParser;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;

import org.terrier.terms.BaseTermPipelineAccessor;
import org.terrier.terms.TermPipelineAccessor;

/**
 * This class encapsulates a query, represented as a set of terms with their query frequencies.
 * By default, all terms are lowercased, unless otherwise specified by the property <tt>lowercase</tt>.
 * Terms encapsulated in a query can be explicitly processed by invoking a term processors pipeline,
 * that must correspond to the same processors pipeline used at indexing time. Term processing is guaranteed
 * to be executed only once: subsequent invocations will cause no further processing.
 */
public class Query extends QueryProperties
{
	private static final SimpleQueryParser parser = new SimpleQueryParser();
	private static TermPipelineAccessor TPA = null;
	
	static {
		final String[] pipes = MatchingConfiguration.get(Property.TERM_PIPELINE).trim().split("\\s*,\\s*");
	
		// The following does not work because of the new FileSystem stuff in org.terrier.utility
		// URL url = Queries.class.getResource("/stopword-list.txt");
		// Setup.setProperty("stopwords.filename", url.toString());
		// The following hack creates a temporary file with the stopword-list.txt contents copied in
		File file = null;
		try {
			InputStream input = SearchRequest.class.getResourceAsStream("/stopword-list.txt");
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

		TPA = new BaseTermPipelineAccessor(pipes);
	}

	private boolean mProcessed = false;
	private String mOriginalQuery;
			
	private Object2IntMap<QueryTerm> mTerms;
	
	/**
	 * Creates a new query object, wrapping the given string as textual query source.
	 * 
	 * @param queryText the input string representing the query (must be not null).
	 * @throws QueryParserException 
	 */
	public Query(final String queryText) throws QueryParserException
	{
		checkNotNull(queryText);
		this.mOriginalQuery = queryText.trim();
		
		this.mTerms 	   = new Object2IntArrayMap<QueryTerm>();
		
		for (QueryTerm qt: parser.parse(mOriginalQuery))
			addTerm(qt);
		
		applyTermPipeline();
	}
	
	/**
	 * Adds a term to the query object.
	 * 
	 * @param t the string representing the term (must be not null)
	 */
	private void addTerm(final QueryTerm sqt)
	{
		checkNotNull(sqt);
		checkNotNull(sqt.getQueryTerm());
		String tmp = sqt.getQueryTerm().trim();
		
		if ("".equals(tmp))
			return;

		int count = mTerms.getInt(sqt);
		sqt.setQueryTerm(tmp);
		mTerms.put(sqt, count + 1);
	}
		
	/**
	 * Return the number of terms of the original query counted with their multiplicities, i.e. the number of query tokens.
	 * 
	 * @return the number of terms of the original query
	 */
	public int getNumberOfTerms()
	{
		int n = 0;
		for (int c: mTerms.values())
			n += c;
		return n;
	}
	
	/**
	 * Return the number of unique terms of the original query.
	 * 
	 * @return the number of unique terms of the original query
	 */
	public int getNumberOfUniqueTerms()
	{
		return mTerms.size();
	}

	/**
	 * Return a copy of the unique terms of the original query.
	 * 
	 * @return an array containing the copies of the original query's unique terms
	 */
	public QueryTerm[] getUniqueTerms()
	{
		return mTerms.keySet().toArray(new QueryTerm[0]);
	}
	
	/**
	 * Return the original textual query.
	 * 
	 * @return the original query (not a copy)
	 */
	public String getOriginalQuery()
	{
		return mOriginalQuery;
	}
	
	/**
	 * Return the multiplicity of a term in the query, i.e., the number of its occurrences.
	 * 
	 * @param t the term to return occurrences of (must be not null)
	 * @return the number of occurrences of input term 
	 */
	public int getTermCount(final QueryTerm qt)
	{
		checkNotNull(qt);
		return mTerms.getInt(qt);
	}
	
	/**
	 * Returns <tt>true</tt> if this query contains no terms.
	 * 
	 * @return <tt>true</tt> if this query contains no terms
	 */
	public boolean isEmpty()
	{
		return mTerms.isEmpty();
	}
	
	/**
	 * Applies a given term pipeline to stored terms, changing the underlying data structure accordingly.
	 * If processed once, further term pipelines will produce no effect.
	 * 
	 * @return <tt>true</tt> if there is at least one term left after processing, <tt>false</tt> otherwise.
	 */
	private boolean applyTermPipeline()
	{	
		if (mProcessed)
			return !mTerms.isEmpty();
		
		Object2IntMap<QueryTerm> newTerms =  new Object2IntArrayMap<QueryTerm>();
		
		for (QueryTerm oldQueryTerm: mTerms.keySet()) {
			final int count = mTerms.getInt(oldQueryTerm);
			String newTerm = TPA.pipelineTerm(oldQueryTerm.getQueryTerm());
			if (newTerm != null && !"".equals(newTerm)) {
				oldQueryTerm.setQueryTerm(newTerm);
				newTerms.put(oldQueryTerm, count);
			}
		}
		
		mTerms = newTerms;
		mProcessed = true;
		return !mTerms.isEmpty();
	}
	
	/**
	 * Returns a string representation of the object.
	 * 
	 * @return a string representation of the object
	 */
	@Override
	public String toString()
	{
		StringBuffer buf = new StringBuffer();
		for (QueryTerm qt: mTerms.keySet()) { 
			buf.append(qt);
			int num = mTerms.getInt(qt);
			if (num != 1)
				buf.append(" (x" + num + ")");
			buf.append(' ');
		}
		return buf.toString();
	}
}