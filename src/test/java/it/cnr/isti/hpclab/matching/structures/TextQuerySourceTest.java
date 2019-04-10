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
import it.cnr.isti.hpclab.matching.structures.query.TextQuerySource;

public class TextQuerySourceTest 
{ 	
	private final int[] qids          = {11, 22, 32, 41, 5 };
	private final String[] queries    = {"a", "b", "c", "d", "e" };
		
	@Test public void testQuerySourceNoQids() throws IOException
	{
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "false");
		MatchingConfiguration.set(Property.THRESHOLD_FILE, "");
		
		File tmpFolder = Files.createTempDir();
		
		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/queries1.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(queries[i]);
			}
			pw.flush();
		}

		TextQuerySource qs = new TextQuerySource(tmpFolder.toString() + "/queries1.txt");
		
		int pos = 0;
		String q;
		while (qs.hasNext()) {
			q = qs.next();
			assertEquals(q, queries[pos++]);
		}
		
		assertFalse(qs.hasNext());
		assertTrue(pos == queries.length);
	}
	
	@Test public void testQuerySourceQids() throws IOException
	{
		MatchingConfiguration.set(Property.HAS_QUERY_ID, "true");
		MatchingConfiguration.set(Property.THRESHOLD_FILE, "");

		File tmpFolder = Files.createTempDir();
		
		try (PrintWriter pw = new PrintWriter(new FileWriter(tmpFolder.toString() + "/queries2.txt"))) {
			for (int i = 0; i < queries.length; i++) {
				pw.println(qids[i] + "\t" + queries[i]);
			}
			pw.flush();
		}

		TextQuerySource qs = new TextQuerySource(tmpFolder.toString() + "/queries2.txt");
		
		int pos = 0;
		String q;
		while (qs.hasNext()) {
			q = qs.next();
			assertEquals(q, queries[pos]);
			assertEquals(qs.getQueryId(), qids[pos]);
			pos++;
		}
		
		assertFalse(qs.hasNext());
		assertTrue(pos == queries.length);	
	}	
}
