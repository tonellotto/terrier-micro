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

	@Test public void testWeights2() throws Exception 
	{
		String toTest = "russian^4.74304222734645E-4 crime^0.3329210579395294 yerin^1.5178071043919772E-4 ministri^2.5211251340806484E-4 crimin^3.7217154749669135E-4 enforc^1.6520200006198138E-4 fight^3.19878599839285E-4 intern^0.33230042457580566 affair^3.150526899844408E-4 organ^0.3327280580997467";
		List<QueryTerm> rtr = parser.parse(toTest);
		assertNotNull(rtr);
	}
}