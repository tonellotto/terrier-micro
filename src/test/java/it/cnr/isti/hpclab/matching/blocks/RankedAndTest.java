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

package it.cnr.isti.hpclab.matching.blocks;

import static org.junit.Assert.assertEquals;
import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.RankedManager;
import it.cnr.isti.hpclab.matching.MatchingSetupTest;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

@RunWith(value = Parameterized.class)
public class RankedAndTest extends MatchingSetupTest
{
	private IndexOnDisk originalIndex = null;
	private IndexOnDisk efIndex = null;

	private IntList docids = new IntArrayList();
	
	private static final String query5 = "page new 1996 1 mail";
	private static final String query4 = "page new 1996 1";
	private static final String query3 = "page new 1996";
	private static final String query1 = "view";
	private static final String query2 = "top view";

	private String query;
	private String model;
	
	/*
	@Parameters(name = "{index}: query={0}")
	public static Collection<Object[]> queries()
	{
		return Arrays.asList(new Object[][] { {query1}, {query2}, {query3}, {query6} });
	}
	*/
	
	@Parameters(name = "{index}: query={0} model={1}")
	public static Collection<Object[]> models_queries()
	{
		return Arrays.asList(new Object[][]	{ 
			{query1,"BM25"},  {query2,"BM25"},  {query3,"BM25"},  {query4,"BM25"},  {query5,"BM25"},
			{query1,"LM"},    {query2,"LM"},    {query3,"LM"},    {query4,"LM"},    {query5,"LM"},
			{query1,"DLH13"}, {query2,"DLH13"}, {query3,"DLH13"}, {query4,"DLH13"}, {query5,"DLH13"}
		});
	}

	
	public RankedAndTest(String query, String model)
	{
		this.query = query;
		this.model = model;
	}

	@BeforeClass public static void init() throws Exception
	{
		MatchingSetupTest.makeEnvironment(true);
		MatchingSetupTest.doWT10GSampleIndexing();
		
		String args[] = {"-path", terrierEtc, 
				 "-prefix", "data" + EliasFano.USUAL_EXTENSION, 
				 "-index", terrierEtc + File.separator + "data.properties", 
				 "-p", Integer.toString(3), "-b"};
		System.setProperty(EliasFano.LOG2QUANTUM, "3");
		Generator.main(args);
	}
	
	@AfterClass public static void clean() 
	{
		MatchingSetupTest.deleteTerrierEtc();
	}
	
	
	@Before public void openIndex() throws IOException
	{
		originalIndex = Index.createIndex();	
		efIndex = Index.createIndex(originalIndex.getPath(), originalIndex.getPrefix() + EliasFano.USUAL_EXTENSION);
	}
	
	@After public void closeIndex() throws IOException
	{
		originalIndex.close();
		efIndex.close();
	}

	private void getAndCorrectResults() throws IOException
	{	
		String terms[] = query.split("\\s+");
		
		LexiconEntry le = originalIndex.getLexicon().getLexiconEntry(terms[0]);
		IterablePosting p = originalIndex.getInvertedIndex().getPostings(le);
		
		while (p.next() != IterablePosting.END_OF_LIST) {
			docids.add(p.getId());
		}
		p.close();
		
		for (int i = 1; i < terms.length; i++) {
			le = originalIndex.getLexicon().getLexiconEntry(terms[i]);
			IntList docids2 = new IntArrayList();
			p = originalIndex.getInvertedIndex().getPostings(le);
		
			while (p.next() != IterablePosting.END_OF_LIST) {
				docids2.add(p.getId());
			}
			p.close();
		
			docids.retainAll(docids2); // docids1 contains the intersection
		}
		System.err.println("Intersection of terms \"" + query + "\" contains " + docids.size() + " documents");
	}
	
	@Test public void originalRankedAnd() throws IOException, QueryParserException
	{		
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		
		getAndCorrectResults();
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.RankedAnd");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    model);
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new RankedManager(originalIndex);
		ResultSet rs = manager.run(srq);
		// manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		Arrays.sort(andDocids);
		for (int i = 0; i < andDocids.length; i++) {
			assertEquals(andDocids[i], docids.getInt(i));
		}
	}
	
	@Test public void eliasFanoRankedAnd() throws IOException, QueryParserException
	{		
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		
		getAndCorrectResults();
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.RankedAnd");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    model);
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new RankedManager(efIndex);
		ResultSet rs = manager.run(srq);
		// manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		Arrays.sort(andDocids);
		for (int i = 0; i < andDocids.length; i++) {
			assertEquals(andDocids[i], docids.getInt(i));
		}
	}

}
