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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.Test;

import com.google.common.io.Files;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import it.cnr.isti.hpclab.matching.structures.query.ThresholdQuerySource;

public class ThresholdQuerySourceTest 
{ 	
	private final String[] qids          = {"11", "22", "32", "41", "5" };
	private final String[] queries    = {"a", "b", "c", "d", "e" };
	private final String[] thresholds = {"0.1", "0.2", "0.3", "0.4", "0.5" };
		
	@Test public void testThresholdsNoQids() throws IOException
	{
		File tmpFolder = Files.createTempDir();
		
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "false");
		MatchingConfiguration.set(Property.THRESHOLD_FILE, tmpFolder.toString() + "/thresholds.txt");
		
		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/queries1.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(queries[i]);
			}
			pw.flush();
		}

		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/thresholds.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(thresholds[i]);
			}
			pw.flush();
		}
		
		ThresholdQuerySource qs = new ThresholdQuerySource(tmpFolder.toString() + "/queries1.txt");
		
		int pos = 0;
		String q;
		while (qs.hasNext()) {
			q = qs.next();
			assertEquals(q, queries[pos]);
			assertEquals(qs.getQueryThreshold(), Float.parseFloat(thresholds[pos]), 1e-6);
			pos++;
		}
		
		assertFalse(qs.hasNext());
		assertTrue(pos == queries.length);
	}

	@Test public void testThresholdsQids() throws IOException
	{	
		File tmpFolder = Files.createTempDir();
	
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "true");
		MatchingConfiguration.set(Property.THRESHOLD_FILE, tmpFolder.toString() + "/thresholds.txt");

		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/queries1.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(qids[i] + "\t" + queries[i]);
			}
			pw.flush();
		}

		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/thresholds.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(qids[i] + "\t"  + thresholds[i]);
			}
			pw.flush();
		}

		MatchingConfiguration.set(Property.THRESHOLD_FILE, tmpFolder.toString() + "/thresholds.txt");
		ThresholdQuerySource qs = new ThresholdQuerySource(tmpFolder.toString() + "/queries1.txt");
		
		int pos = 0;
		String q;
		while (qs.hasNext()) {
			q = qs.next();
			assertEquals(q, queries[pos]);
			assertEquals(qs.getQueryId(), qids[pos]);
			assertEquals(qs.getQueryThreshold(), Float.parseFloat(thresholds[pos]), 1e-6);
			pos++;
		}
		
		assertFalse(qs.hasNext());
		assertTrue(pos == queries.length);
	}

}
