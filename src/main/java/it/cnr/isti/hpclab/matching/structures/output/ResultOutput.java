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

package it.cnr.isti.hpclab.matching.structures.output;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;

import it.cnr.isti.hpclab.matching.structures.SearchRequest;

public abstract class ResultOutput implements Closeable
{
	protected static final Logger LOGGER = Logger.getLogger(ResultOutput.class);
	
	public static ResultOutput newInstance(String type)
	{
		if (type == null || "null".equals(type)) {
			return new NullResultOutput();
		} else if ("docid".equalsIgnoreCase(type)) {
			return new DocidResultOutput();
		} else if ("score".equalsIgnoreCase(type)) {
			return new ScoredResultOutput();
		} else if ("docno".equalsIgnoreCase(type)) {
			return new DocnoResultOutput();
		} else if ("trec".equalsIgnoreCase(type)) {
			return new TrecResultOutput();
		} 
 

		throw new IllegalArgumentException("Unknown ResultOutput type");
	}
	
	/** 
	 * Outputs the results of search request rq
	 * 
	 * @param re the search request
	 */
	public abstract void print(final SearchRequest rq) throws IOException;
}
