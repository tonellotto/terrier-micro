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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Test;

import it.cnr.isti.hpclab.matching.structures.resultset.DocidResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;

public class ResultSetTest 
{
	@Test public void TestEmptyResultSet() 
	{
		ResultSet rs = new EmptyResultSet();
		
		assertNotNull(rs);
		assertNotNull(rs.docids());
		assertNotNull(rs.scores());
		
		assertEquals(rs.size(), 0);
		assertEquals(rs.docids().length, 0);
		assertEquals(rs.scores().length, 0);
	}
	
	@Test public void TestDocidResultSet()
	{
		Integer docids[] = {1,2,4,7,9,11,2,3,0}; 
		ResultSet rs = new DocidResultSet(new ArrayList<Integer>(Arrays.asList(docids)));
		
		assertNotNull(rs);
		assertNotNull(rs.docids());
		Throwable caught = null;
		try {
			rs.scores();
		} catch (Throwable t) {
		   caught = t;
		}
		assertNotNull(caught);
		assertSame(UnsupportedOperationException.class, caught.getClass());
		
		
		assertEquals(rs.size(), docids.length);
		assertEquals(rs.docids().length, docids.length);
		
		for (int i = 0; i < docids.length; i++) {
			assertEquals(docids[i].intValue(), rs.docids()[i]);
		}
	}
	
	@Test public void TestScoredResultSet()
	{
		Integer docids[] = {1,2,4,7,9,11,2,3,0};
		Float  scores[] = {0.1f,0.2f,0.4f,0.7f,0.9f,0.11f,0.2f,0.3f,0.0f};
		ResultSet rs = new ScoredResultSet(new ArrayList<Integer>(Arrays.asList(docids)),new ArrayList<Float>(Arrays.asList(scores)));
		
		assertNotNull(rs);
		assertNotNull(rs.docids());
		assertNotNull(rs.scores());
		
		assertEquals(rs.size(), docids.length);
		assertEquals(rs.docids().length, docids.length);
		assertEquals(rs.size(), scores.length);
		assertEquals(rs.scores().length, scores.length);

		
		for (int i = 0; i < docids.length; i++) {
			assertEquals(docids[i].intValue(), rs.docids()[i]);
			assertEquals(scores[i].doubleValue(), rs.scores()[i], Double.MIN_VALUE);
		}
	}

}
