package it.cnr.isti.hpclab.matching.structures;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;

public class QueryTermTest 
{
	@Test public void test1()
	{
		QueryTerm qt = new QueryTerm("pippo");
		
		assertEquals(qt.getQueryTerm(), "pippo");
		assertEquals(qt.isRequired(), false);
		assertEquals(qt.getWeight(), 1.0f, 1e-6);
	}
	
	@Test public void test2()
	{
		QueryTerm qt = new QueryTerm("pippo", 3.6f);
		
		assertEquals(qt.getQueryTerm(), "pippo");
		assertEquals(qt.isRequired(), false);
		assertEquals(qt.getWeight(), 3.6f, 1e-6);
	}

	@Test public void test3()
	{
		QueryTerm qt = new QueryTerm("pippo", true);
		
		assertEquals(qt.getQueryTerm(), "pippo");
		assertEquals(qt.isRequired(), true);
		assertEquals(qt.getWeight(), 1.0f, 1e-6);
	}

	@Test public void test4()
	{
		QueryTerm qt = new QueryTerm("pippo", true, 3.6f);
		
		assertEquals(qt.getQueryTerm(), "pippo");
		assertEquals(qt.isRequired(), true);
		assertEquals(qt.getWeight(), 3.6f, 1e-6);
	}
	
	@Test public void test5()
	{
		QueryTerm qt1 = new QueryTerm("pippo");
		QueryTerm qt2 = new QueryTerm("pippo");
		
		assertEquals(qt1, qt2);
	}
}
