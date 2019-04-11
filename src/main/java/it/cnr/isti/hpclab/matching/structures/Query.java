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

import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectMap;

import org.terrier.terms.TermPipelineAccessor;

/**
 * This class encapsulates a query, represented as a set of terms with their query frequencies.
 * By default, all terms are lowercased, unless otherwise specified by the property <tt>lowercase</tt>.
 * Terms encapsulated in a query can be explicitly processed by invoking a term processors pipeline,
 * that must correspond to the same processors pipeline used at indexing time. Term processing is guaranteed
 * to be executed only once: subsequent invocations will cause no further processing.
 */
public class Query 
{	
	public enum RuntimeProperty 
	{
		PROCESSING_TIME 	("processing.time"),
		QUERY_LENGTH    	("query.length"),
		PROCESSED_POSTINGS 	("processed.postings"),
		PARTIALLY_PROCESSED_DOCUMENTS 	("partially.processed.documents"),
		NUM_PIVOTS					 	("num.pivots"),
		TOTAL_POSTINGS		("total.postings"),
		PROCESSED_TERMS 	("processed.terms"),
		PROCESSED_TERMS_DF 	("processed.terms.df"),
		PROCESSED_TERMS_MS 	("processed.terms.ms"),
		
		INITIAL_THRESHOLD 	("initial.threshold"),
		FINAL_THRESHOLD 	("final.threshold"),
	
		QUERY_TERMS			("query.terms"),
				
		NULL                ("");
		  
		private final String mName;
		
		private RuntimeProperty(final String name)
		{
			mName = name;
		}
		
		@Override
		public String toString()
		{
			return mName;
		}		
	}
	
	private boolean mProcessed = false;
	private String mOriginalQuery;
			
	private Object2IntMap<String> mTerms;
	private Object2ObjectMap<RuntimeProperty, String> mMetadata;
	private Object2ObjectMap<String, String> mMetadata_str;
	
	/**
	 * Creates a new query object, wrapping the given string as textual query source.
	 * 
	 * @param queryText the input string representing the query (must be not null).
	 */
	public Query(final String queryText)
	{
		checkNotNull(queryText);
		this.mOriginalQuery = queryText.trim();
		
		this.mTerms =  new Object2IntArrayMap<String>();
		this.mMetadata     = new Object2ObjectArrayMap<>();
		this.mMetadata_str = new Object2ObjectArrayMap<>();
		
		int pos = 0, end;
        while ((end = queryText.indexOf(' ', pos)) >= 0) {
        	addTerm(queryText.substring(pos, end));
            pos = end + 1;
        }
        addTerm(queryText.substring(pos));
	}
	
	/**
	 * Adds a term to the query object.
	 * 
	 * @param t the string representing the term (must be not null)
	 */
	private void addTerm(final String t)
	{
		checkNotNull(t);
		String tmp = t.trim();
		
		if (tmp.equals(""))
			return;

		int count = mTerms.getInt(tmp);
		mTerms.put(tmp, count + 1);
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
	public String[] getUniqueTerms()
	{
		return mTerms.keySet().toArray(new String[0]);
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
	public int getTermCount(final String t)
	{
		checkNotNull(t);
		return mTerms.getInt(t);
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
	 * @param tpa the term pipeline to apply (must be not null)
	 * 
	 * @return <tt>true</tt> if there is at least one term left after processing, <tt>false</tt> otherwise.
	 */
	public boolean applyTermPipeline(final TermPipelineAccessor tpa)
	{
		checkNotNull(tpa);
		
		if (mProcessed)
			return !mTerms.isEmpty();
		
		Object2IntMap<String> newTerms =  new Object2IntArrayMap<String>();
		
		for (String oldTerm: mTerms.keySet()) {
			String newTerm = tpa.pipelineTerm(oldTerm);
			if (newTerm != null && !newTerm.equals(""))
				newTerms.put(newTerm, mTerms.getInt(oldTerm));
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
		for (String term: mTerms.keySet()) { 
			buf.append(term);
			int num = mTerms.getInt(term);
			if (num != 1)
				buf.append(" (x" + num + ")");
			buf.append(" ");
		}
		return buf.toString();
	}
	
	/**
	 * Add a custom metadata to the object, as a key value pair of strings.
	 * 
	 * @param key the key
	 * @param value the value
	 */
	public void addMetadata(final RuntimeProperty key, final String value)
	{
		// checkNotNull(key);
		checkNotNull(value);
		mMetadata.put(key, value);
	}

	public void addMetadata(final String key, final String value)
	{
		checkNotNull(key);
		checkNotNull(value);
		mMetadata_str.put(key, value);
	}

	/**
	 * Returns the value associated to the specified key.

	 * @param key the key
	 * @return the corresponding value, or <tt>null</tt> if no value was present for the given key
	 */
	public String getMetadata(final RuntimeProperty key)
	{
		// checkNotNull(key);
		return mMetadata.get(key);
	}
	
	public String getMetadata(final String key)
	{
		checkNotNull(key);
		return mMetadata_str.get(key);
	}

}
