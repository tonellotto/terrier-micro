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

package it.cnr.isti.hpclab.matching.util;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.terrier.structures.Index;
import org.terrier.utility.ApplicationSetup;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import it.cnr.isti.hpclab.manager.BooleanManager;
import it.cnr.isti.hpclab.manager.Manager;

import it.cnr.isti.hpclab.matching.structures.Query.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;

import it.cnr.isti.hpclab.matching.util.IntUtils.ClusteredGenerator;

public class Benchmark 
{
	@ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();

	private static Index originalIndex = null;
	private static Index efIndex = null;
	private static final DecimalFormat df = new DecimalFormat("0.###");

	private static final int MAX_DOCS = 1_000_000;
	
	private static final int COMMON_SIZE = 1_000;
	
	private static final int MAX_TERMS = 100;
	
	private static final int MIN_SIZE = 10_000;
	
	private static final int MAX_SIZE = (int) (0.1 * MAX_DOCS);

	@BeforeClass
	public static void createIndexes() throws IOException
	{
		final int[] PL_SIZE = new int[MAX_TERMS];
		
		final int GAP = (MAX_SIZE - MIN_SIZE) / (MAX_TERMS - 1);
		
		for (int i = 0; i < MAX_TERMS; ++i)
			PL_SIZE[i] = MIN_SIZE + i * GAP;
				
		int[] common = (new ClusteredGenerator()).generate(COMMON_SIZE, MAX_DOCS);
		
		int[][] x = new int[MAX_TERMS][];
		for (int i = 0; i < x.length; ++i)
			x[i] = IntUtils.union(common, (new ClusteredGenerator()).generate(PL_SIZE[i], MAX_DOCS));

		ApplicationSetup.TERRIER_INDEX_PATH = tempFolder.getRoot().toString();
		ApplicationSetup.TERRIER_INDEX_PREFIX = "data";

		originalIndex = IndexUtils.makeTerrierIndex(x);
		efIndex = IndexUtils.makeEFIndex(originalIndex);
	}
	
	@AfterClass
	public static void closeIndexes() throws IOException
	{
		originalIndex.close();
		efIndex.close();
	}
	
	@Test
	public void test_o_2() throws IOException
	{	
		System.err.println(IndexUtils.getInvertedIndexSize(originalIndex));
		
		MatchingConfiguration.set(Property.TOP_K, "1000");
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.And");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");

		Manager manager = new BooleanManager(originalIndex);
		
		double[][] runs = new double[MAX_TERMS][];
		for (int i = 0; i < MAX_TERMS; ++i)
			runs[i] = new double[100];
		
		for (int repeat = 0; repeat < 100; ++repeat) {
			for (int i = 0; i < MAX_TERMS - 1; ++i) {
				SearchRequest srq = new SearchRequest(1, String.valueOf(i) + " " + String.valueOf(MAX_TERMS - 1));
				manager.run(srq);
				runs[i][repeat] = Double.parseDouble(srq.getQuery().getMetadata(RuntimeProperty.PROCESSING_TIME));
			}
		}
		
		for (int i = 0; i < MAX_TERMS; ++i) {
			System.out.println("OR " + i + " " + df.format(Arrays.stream(runs[i]).average().getAsDouble()));
		}
		
		manager.close();
	}
	
	@Test
	public void test_e_2() throws IOException
	{ 
		System.err.println(IndexUtils.getInvertedIndexSize(efIndex));
		
		MatchingConfiguration.set(Property.TOP_K, "1000");
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.And");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");

		Manager manager = new BooleanManager(efIndex);
		
		double[][] runs = new double[MAX_TERMS][];
		for (int i = 0; i < MAX_TERMS; ++i)
			runs[i] = new double[100];
		
		for (int repeat = 0; repeat < 100; ++repeat) {
			for (int i = 0; i < MAX_TERMS - 1; ++i) {
				SearchRequest srq = new SearchRequest(1, String.valueOf(i) + " " + String.valueOf(MAX_TERMS - 1));
				manager.run(srq);
				runs[i][repeat] = Double.parseDouble(srq.getQuery().getMetadata(RuntimeProperty.PROCESSING_TIME));
			}
		}
		
		for (int i = 0; i < MAX_TERMS; ++i) {
			System.out.println("EF " + i + " " + df.format(Arrays.stream(runs[i]).average().getAsDouble()));
		}
		
		manager.close();
	}

}
