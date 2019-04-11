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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;

import it.cnr.isti.hpclab.Retrieve;
import it.cnr.isti.hpclab.parallel.ParallelRetrieve;

// TODO: test more than 1 query at a time...

@RunWith(value = Parameterized.class)
public class ParallelTest extends MatchingSetupTest
{
	private static class Pair implements Comparable<Pair>
	{
		public String docno;
		public final float score;
		
		public Pair(String line)
		{
			String[] tok = line.split("\\s+");
			docno = tok[1];
			score = Float.parseFloat(tok[2]);
		}
		@Override
		public int compareTo(Pair o) 
		{
			return docno.compareTo(o.docno);
		}
		
	}

	private static final String query5 = "page new 1996 1 mail";
	private static final String query4 = "page new 1996 1";
	private static final String query3 = "page new 1996";
	private static final String query1 = "view";
	private static final String query2 = "top view";

	private String query;
	private String model;
	
	@Parameters(name = "{index}: query={0} model={1}")
	public static Collection<Object[]> models_queries()
	{
/*
		return Arrays.asList(new Object[][]	{ 
			{query5,"BM25"},
		});
*/
		return Arrays.asList(new Object[][]	{ 
			{query1,"BM25"},  {query2,"BM25"},  {query3,"BM25"},  {query4,"BM25"},  {query5,"BM25"},
			{query1,"LM"},    {query2,"LM"},    {query3,"LM"},    {query4,"LM"},    {query5,"LM"},
			{query1,"DLH13"}, {query2,"DLH13"}, {query3,"DLH13"}, {query4,"DLH13"}, {query5,"DLH13"}
		});
	}
	
	public ParallelTest(String query, String model)
	{
		this.query = query;
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

	private List<Pair> original_results() throws IOException
	{
		Files.write(Paths.get(terrierEtc + File.separator + "./query.txt"), query.getBytes());
		
		MatchingConfiguration.set(Property.QUERY_SOURCE_CLASSNAME, "it.cnr.isti.hpclab.matching.structures.query.TextQuerySource");
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "false");
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME, model);
		MatchingConfiguration.set(Property.INDEX_PATH, terrierEtc);
		MatchingConfiguration.set(Property.INDEX_PREFIX, "data.ef");
		MatchingConfiguration.set(Property.TOP_K, "100000");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.RankedOr");
		MatchingConfiguration.set(Property.QUERY_FILE, terrierEtc + File.separator + "query.txt");
		
		MatchingConfiguration.set(Property.RESULTS_FILENAME, terrierEtc + File.separator + "results.txt");
		MatchingConfiguration.set(Property.RESULTS_OUTPUT_TYPE, "docno");
		
		Retrieve.main(new String[] {"y"});

		try (Stream<String> stream = Files.lines(Paths.get(terrierEtc + File.separator + "results.txt"))) {
			return stream.map(x -> new Pair(x)).sorted().collect(Collectors.toList());
		}
	}
	
	private List<Pair> parallel_results() throws IOException
	{
		Files.write(Paths.get(terrierEtc + File.separator + "./query.txt"), query.getBytes());
		
		MatchingConfiguration.set(Property.QUERY_SOURCE_CLASSNAME, "it.cnr.isti.hpclab.matching.structures.query.TextQuerySource");
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "false");
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME, model);
		MatchingConfiguration.set(Property.INDEX_PATH, terrierEtc);
		MatchingConfiguration.set(Property.INDEX_PREFIX, "data.ef");
		MatchingConfiguration.set(Property.TOP_K, "100000");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.RankedOr");
		MatchingConfiguration.set(Property.QUERY_FILE, terrierEtc + File.separator + "query.txt");
		
		MatchingConfiguration.set(Property.RESULTS_FILENAME, terrierEtc + File.separator + "results.txt");
		MatchingConfiguration.set(Property.RESULTS_OUTPUT_TYPE, "docno");
		
		ParallelRetrieve.main(new String[] {"y"});

		try (Stream<String> stream = Files.lines(Paths.get(terrierEtc + File.separator + "results.txt"))) {
			return stream.map(x -> new Pair(x)).sorted().collect(Collectors.toList());
		}
	}

	@Test
	public void test() throws IOException
	{
		List<Pair> original = original_results();
		List<Pair> parallel = parallel_results();
		
		assertNotNull(original);
		assertNotNull(parallel);
		
		assertEquals(original.size(), parallel.size());
		
		Pair p1, p2;
		for (int i = 0; i < original.size(); ++i) {
			p1 = original.get(i);
			p2 = parallel.get(i);
			
			assertEquals(p1.docno, p2.docno);
			assertEquals(p1.score, p2.score, 1e-4);
		}
	}
}

