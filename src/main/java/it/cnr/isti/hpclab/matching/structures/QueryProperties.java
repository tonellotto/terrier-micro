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

import java.util.Map;

import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;

public class QueryProperties 
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
		NUM_RESULTS 		("num.results"),
		
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
	
	protected Map<RuntimeProperty, String> mMetadata;
	protected Map<String, String> mMetadata_str;
	
	public QueryProperties()
	{
		this.mMetadata     = new Object2ObjectArrayMap<>();
		this.mMetadata_str = new Object2ObjectArrayMap<>();		
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
