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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import org.terrier.terms.BaseTermPipelineAccessor;
import org.terrier.terms.TermPipelineAccessor;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * This class wraps together a query and its results for caching purposes. It is also responsible for processing the
 * original terms of a query, for example through a pipeline of stemmer and stopword removal.
 * 
 * @author Nicola Tonellotto
 */
public class SearchRequest 
{
	private Query  mQueryObject;
	private static TermPipelineAccessor TPA = null;
	
	protected int    mQueryId;
	protected ResultSet mResultSet = null;

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

	/**
	 * Constructor for inheritance purposes
	 */
	public SearchRequest()
	{
	}
	
	/**
	 * Creates a new search request object, applying the statically-derived term pipeline.
	 * 
	 * @param queryId the query id, used by <tt>trec_eval</tt> inputs (must be not negative)
	 * @param queryText queryText the input string representing the query (must be not null).
	 */
	public SearchRequest(int queryId, final String queryText)
	{
		checkNotNull(queryText);
		checkArgument(queryId >= 0);
		
		this.mQueryId = queryId;	
		this.mQueryObject = new Query(queryText);
		this.mQueryObject.applyTermPipeline(TPA);
	}
	
	/**
	 * Return the result set associated with the query, if the query has been processed.
	 * 
	 * @return the result set associated with the query, if the query has been processed, or <tt>null</tt> otherwise.
	 */
	public ResultSet getResultSet()
	{
		return mResultSet;
	}
	
	/**
	 * Set the result set associated with the query.
	 * 
	 * @param results the result set associated with the query (must be not null)
	 */
	public void setResultSet(ResultSet results)
	{
		checkNotNull(results);
		mResultSet = results;
	}

	/**
	 * Return the query id associated with the stored query.
	 * 
	 * @return the query id associated with the stored query
	 */
	public int getQueryId()
	{
		return mQueryId;
	}

	/**
	 * Return the stored query.
	 * 
	 * @return the stored query
	 */
	public Query getQuery()
	{
		return mQueryObject;
	}

	/**
	 * Returns <tt>true</tt> if the stored query contains no terms.
	 * 
	 * @return <tt>true</tt> if the stored query contains no terms
	 */
	public boolean isEmpty()
	{
		return mQueryObject.isEmpty();
	}

	/**
	 * Return a copy of the unique terms of the original query.
	 * 
	 * @return an array containing the copies of the original query's unique terms
	 */
	public String[] getQueryTerms()
	{
		return mQueryObject.getUniqueTerms();
	}

	/**
	 * Return the original textual query.
	 * 
	 * @return the original query (not a copy)
	 */
	public String getOriginalQuery()
	{
		return mQueryObject.getOriginalQuery();
	}	
}
