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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;
import org.terrier.utility.ApplicationSetup;

import it.cnr.isti.hpclab.matching.util.IntUtils.ClusteredGenerator;

public class IndexUtilsTest 
{
	@Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();
	
	@Test
	public void test() throws IOException
	{
		ApplicationSetup.TERRIER_INDEX_PATH = tempFolder.getRoot().toString();
		ApplicationSetup.TERRIER_INDEX_PREFIX = "data";
				
		int[][] pls = {{0,1,2,3,4},{0,2,4},{1,3}};

		/*
		 0 -> 0 1
		 1 -> 0 2
		 2 -> 0 1
		 3 -> 0 2
		 4 -> 0 1
		 */
		Index index = IndexUtils.makeTerrierIndex(pls);
		
		assertEquals( 5, index.getCollectionStatistics().getNumberOfDocuments());
		assertEquals( 3, index.getCollectionStatistics().getNumberOfUniqueTerms());
		assertEquals(10, index.getCollectionStatistics().getNumberOfTokens());
		assertEquals(10, index.getCollectionStatistics().getNumberOfPointers());

		for (int termid = 0; termid < index.getCollectionStatistics().getNumberOfUniqueTerms(); ++termid) {
			LexiconEntry le = index.getLexicon().getLexiconEntry(termid).getValue();
			int[] tmp = new int[le.getDocumentFrequency()];
			int pos = 0;
			IterablePosting p = index.getInvertedIndex().getPostings(le);
			while (p.next() != IterablePosting.EOL)
				tmp[pos++] = p.getId();
			
			assertArrayEquals(tmp, pls[termid]);
		}
		
		index.close();
	}

	@Test
	public void test_small_index() throws IOException
	{
		ApplicationSetup.TERRIER_INDEX_PATH = tempFolder.getRoot().toString();
		ApplicationSetup.TERRIER_INDEX_PREFIX = "data";
				
		final int MAX = 1_000_000;
		final int COMMON_SIZE = 100_000;
		
		final int NUM_TERMS = 5;
		final int[] PL_SIZE = {30_000, 40_000, 50_000, 50_000, 100_000};
		
		int[] common = (new ClusteredGenerator()).generate(COMMON_SIZE, MAX);
		
		int[][] pls = new int[NUM_TERMS][];
		for (int i = 0; i < pls.length; ++i)
			pls[i] = IntUtils.union(common, (new ClusteredGenerator()).generate(PL_SIZE[i], MAX));

		Index index = IndexUtils.makeTerrierIndex(pls);

		for (int termid = 0; termid < index.getCollectionStatistics().getNumberOfUniqueTerms(); ++termid) {
			LexiconEntry le = index.getLexicon().getLexiconEntry(termid).getValue();
			assertEquals(le.getDocumentFrequency(), pls[termid].length);
			IterablePosting p = index.getInvertedIndex().getPostings(le);
			int pos = 0;
			while (p.next() != IterablePosting.EOL)
				assertEquals(pls[termid][pos++], p.getId());
			assertTrue(p.endOfPostings());
			assertEquals(pos, pls[termid].length);
		}
		
		index.close();
	}

}
