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

import static org.junit.Assert.assertEquals;

import java.net.URL;

import org.junit.Before;
import org.junit.Test;
// import org.terrier.terms.BaseTermPipelineAccessor;
// import org.terrier.terms.TermPipelineAccessor;

import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;

public class QueryTest 
{
	private static final String query1 = "opinionated";
	private static final String query2 = "definition gravitational";
	private static final String query3 = "obama family tree";
	private static final String query4 = "obama family tree obama family tree";
	// private static final String query5 = "a fox can not know the black fox fox";

	@Before
	public void LoadPipeline()
	{
		System.setProperty("terrier.home", System.getProperty("user.dir"));
		
		// final String[] pipes = System.getProperty("termpipelines", "Stopwords,PorterStemmer").trim().split("\\s*,\\s*");
		
		URL url = Query.class.getResource("/stopword-list.txt");
		System.setProperty("stopwords.filename", url.toString());
		// tpa = new BaseTermPipelineAccessor(pipes);		
	}

	@Test public void testQuery1() throws QueryParserException
	{		
		Query q = new Query(query1);
		assertEquals(query1, q.getOriginalQuery());
		assertEquals(q.getNumberOfTerms(), 1);
		assertEquals(q.getNumberOfUniqueTerms(), 1);
		System.out.println(q + " [original is " + q.getOriginalQuery() + "]");
	}
	
	@Test public void testQuery2() throws QueryParserException
	{		
		Query q = new Query(query2);
		assertEquals(query2, q.getOriginalQuery());
		assertEquals(q.getNumberOfTerms(), 2);
		assertEquals(q.getNumberOfUniqueTerms(), 2);
		System.out.println(q + " [original is " + q.getOriginalQuery() + "]");
	}

	@Test public void testQuery3() throws QueryParserException
	{		
		Query q = new Query(query3);
		assertEquals(query3, q.getOriginalQuery());
		assertEquals(q.getNumberOfTerms(), 3);
		assertEquals(q.getNumberOfUniqueTerms(), 3);
		System.out.println(q + " [original is " + q.getOriginalQuery() + "]");
	}

	@Test public void testQuery4() throws QueryParserException
	{		
		Query q = new Query(query4);
		assertEquals(query4, q.getOriginalQuery());
		assertEquals(q.getNumberOfTerms(), 6);
		assertEquals(q.getNumberOfUniqueTerms(), 3);
		System.out.println(q + " [original is " + q.getOriginalQuery() + "]");
	}
}
