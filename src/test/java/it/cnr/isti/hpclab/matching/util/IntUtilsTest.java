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

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import it.cnr.isti.hpclab.matching.util.IntUtils.ClusteredGenerator;

public class IntUtilsTest 
{
	@Test
	public void test_intersection2()
	{
		final int MAX = 1000;
		final int COMMON_SIZE = 900;
		int[] common = (new ClusteredGenerator()).generate(COMMON_SIZE, MAX);
		
		int[] x1 = IntUtils.union(common, (new ClusteredGenerator()).generate(1, 0, MAX));
		int[] x2 = IntUtils.union(common, (new ClusteredGenerator()).generate(1, MAX, MAX << 1));
		
		int[] intersection = IntUtils.intersection(x1, x2);
		
		assertEquals(common.length, intersection.length);
		assertArrayEquals(common, intersection);
	}

	@Test
	public void test_intersection5()
	{
		final int MAX = 500_000;
		final int COMMON_SIZE = 1_000;
		
		final int NUM_TERMS = 5;
		final int[] PL_SIZE = {30_000, 40_000, 50_000, 50_0000, 100_000};
		
		int[] common = (new ClusteredGenerator()).generate(COMMON_SIZE, MAX);
		
		int[][] x = new int[NUM_TERMS][];
		for (int i = 0; i < x.length; ++i)
			x[i] = IntUtils.union(common, (new ClusteredGenerator()).generate(PL_SIZE[i], MAX));

		int[] intersection = IntUtils.intersection(x);
		System.out.println(IntUtilsTest.class.getName() + ": " + intersection.length + ", " + common.length);
		assertTrue(intersection.length >= common.length);
		
		for (int docid: common)
			assertTrue(ArrayUtils.contains(intersection, docid));
	}
}
