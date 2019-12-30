package it.cnr.isti.hpclab.matching.structures.query;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class TestSimpleQueryParser 
{
	private static SimpleQueryParser parser = new SimpleQueryParser();
	
	@Test public void testSimpleTerm() throws Exception 
	{
		String toTest = "a";
		List<QueryTerm> rtr = parser.parse(toTest);
		assertNotNull(rtr);
		assertEquals(rtr.size(), 1);
		assertTrue(rtr.get(0) instanceof QueryTerm);
	}

	@Test public void testSimpleTerms() throws Exception 
	{		
		String toTest = "a b";
		List<QueryTerm> rtr = parser.parse(toTest);
		assertNotNull(rtr);
		assertEquals(rtr.size(), 2);
		assertTrue(rtr.get(0) instanceof QueryTerm);
		assertTrue(rtr.get(1) instanceof QueryTerm);
	}

	@Test public void testRequiredSimple() throws Exception 
	{
		String toTest = "a +c";
		List<QueryTerm> rtr = parser.parse(toTest);
		assertNotNull(rtr);
		assertEquals(rtr.size(), 2);
		assertTrue(rtr.get(0) instanceof QueryTerm);
		assertFalse(rtr.get(0).isRequired());
		assertTrue(rtr.get(1).isRequired());
	}

	@Test public void testWeights() throws Exception 
	{
		String toTest = "+obama family^0.1 +tree^0.98";
		List<QueryTerm> rtr = parser.parse(toTest);
		assertNotNull(rtr);
		assertEquals(rtr.size(), 3);
		assertTrue(rtr.get(0).isRequired());
		assertFalse(rtr.get(1).isRequired());
		assertTrue(rtr.get(2).isRequired());
		assertEquals(rtr.get(0).getWeight(), 1.0, 1e-6);
		assertEquals(rtr.get(1).getWeight(), 0.1, 1e-6);
		assertEquals(rtr.get(2).getWeight(), 0.98, 1e-6);
		
		assertEquals((rtr.get(0)).getQueryTerm(), "obama");
		assertEquals((rtr.get(1)).getQueryTerm(), "family");
		assertEquals((rtr.get(2)).getQueryTerm(), "tree");
	}
}