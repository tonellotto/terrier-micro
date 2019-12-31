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

package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.RankedManager;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.query.QueryParserException;
import it.cnr.isti.hpclab.maxscore.MSGenerator;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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

@RunWith(value = Parameterized.class)
public class MultiTermQueryTest extends MatchingSetupTest
{
	private IndexOnDisk efIndex = null;

	private static final String query1 = "view";
	private static final String query2 = "top view";

	private String q1, q2;
	private String model;
	
	@Parameters(name = "{index}: query={0} model={1}")
	public static Collection<Object[]> models_queries()
	{
		/*
		return Arrays.asList(new Object[][]	{ 
			{query1,"BM25"},
		});
		*/
				
		return Arrays.asList(new Object[][]	{ 
			{query1,"BM25"},  {query2,"BM25"},
			{query1,"LM"},    {query2,"LM"},  
			{query1,"DLH13"}, {query2,"DLH13"}
		});
	}
	
	public MultiTermQueryTest(String query, String model)
	{
		this.q1 = query;
		this.q2 = query + " " + query;
		this.model = model;
	}

	@BeforeClass public static void init() throws Exception
	{
		MatchingSetupTest.makeEnvironment();
		MatchingSetupTest.doWT10GSampleIndexing();
		
		String args[] = {"-path", terrierEtc, 
				 "-prefix", "data" + EliasFano.USUAL_EXTENSION, 
				 "-index", terrierEtc + File.separator + "data.properties", 
				 "-p", Integer.toString(3)};
		System.setProperty(EliasFano.LOG2QUANTUM, "3");
		Generator.main(args);
	}
	
	@AfterClass public static void clean() 
	{
		MatchingSetupTest.deleteTerrierEtc();
	}
	
	@Before public void openIndex() throws IOException
	{
		efIndex = Index.createIndex(terrierEtc, "data" + EliasFano.USUAL_EXTENSION);
	}
	
	@After public void closeIndex() throws IOException
	{
		efIndex.close();
	}
	
	@Before public void createMaxScoreIndex() throws IOException, InterruptedException
	{
		if (Files.exists(Paths.get(terrierEtc + File.separator + "data.ef" + MaxScoreIndex.USUAL_EXTENSION)))
			Files.delete(Paths.get(terrierEtc + File.separator + "data.ef" + MaxScoreIndex.USUAL_EXTENSION));
		String argsMS[] = {"-index", terrierEtc + File.separator + "data.ef.properties",
						 "-wm", ("BM25Fast".equals(model)) ? "BM25": model,
				 		 "-p", Integer.toString(3)};

		MSGenerator.main(argsMS);
	}
	
	private ResultSet getRankedResults(final String query) throws IOException, QueryParserException
	{
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.MaxScore");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME, model);
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new RankedManager(efIndex);
		ResultSet rs = manager.run(srq);

		return rs;
	}

	@Test public void rankFull() throws IOException, QueryParserException
	{	
		MatchingConfiguration.set(Property.TOP_K, "10000");
		
		ResultSet correct1 = getRankedResults(q1);
		ResultSet correct2 = getRankedResults(q2);
			
		assertEquals(correct1.size(), correct2.size());
		assertArrayEquals(correct1.docids(), correct2.docids());
		
		for (int i = 0; i < correct1.size(); ++i)
			assertEquals(correct1.scores()[i] * 2, correct2.scores()[i], 1e-5);
		
		// MatchingSetupTest.Compare(correct, current);	
	}	
}
