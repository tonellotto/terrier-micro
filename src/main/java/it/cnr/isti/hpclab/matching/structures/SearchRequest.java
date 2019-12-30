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

import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;

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
	
	protected int    mQueryId;
	protected ResultSet mResultSet = null;

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
	 * @param queryText queryText the input string representing the query (must be not null)
	 * 
	 * @throws QueryParserException 
	 */
	public SearchRequest(int queryId, final String queryText) throws QueryParserException
	{
		checkNotNull(queryText);
		checkArgument(queryId >= 0);
		
		this.mQueryId = queryId;	
		this.mQueryObject = new Query(queryText);
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
	public QueryTerm[] getQueryTerms()
	{
		return mQueryObject.getUniqueTerms();
	}

	/**
	 * Return the number of occurrences of the query term in the original query
	 * 
	 * @return the number of occurrences of the query term in the original query
	 */
	public int getQueryTermFrequency(final QueryTerm queryTerm)
	{
		return mQueryObject.getTermCount(queryTerm);
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
