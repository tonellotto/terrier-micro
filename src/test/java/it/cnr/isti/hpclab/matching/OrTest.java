package it.cnr.isti.hpclab.matching;

import static org.junit.Assert.assertEquals;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;
import it.cnr.isti.hpclab.manager.BooleanManager;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

@RunWith(value = Parameterized.class)
public class OrTest extends MatchingSetupTest
{
	private IndexOnDisk originalIndex = null;
	private IndexOnDisk efIndex = null;

	private IntList docids = new IntArrayList();
	
	private static final String query5 = "page new 1996 1 mail";
	private static final String query4 = "page new 1996 1";
	private static final String query3 = "page new 1996";
	private static final String query1 = "view";
	private static final String query2 = "top view";

	private String query;
	
	@Parameters(name = "{index}: query={0}")
	public static Collection<Object[]> skipSizeValues()
	{
		return Arrays.asList(new Object[][] { {query1}, {query2}, {query3}, {query4}, {query5} });
	}
	
	public OrTest(String query)
	{
		this.query = query;
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
	
	@AfterClass public static void clean() 
	{
		MatchingSetupTest.deleteTerrierEtc();
	}
	
	@Before public void openIndex() throws IOException
	{
		originalIndex = Index.createIndex();	
		efIndex = Index.createIndex(originalIndex.getPath(), originalIndex.getPrefix() + EliasFano.USUAL_EXTENSION);	
	}
	
	@After public void closeIndex() throws IOException
	{
		originalIndex.close();
		efIndex.close();
	}
	
	private void getOrCorrectResults() throws IOException
	{	
		String terms[] = query.split("\\s+");
		
		LexiconEntry le = originalIndex.getLexicon().getLexiconEntry(terms[0]);
		IterablePosting p = originalIndex.getInvertedIndex().getPostings(le);
		
		while (p.next() != IterablePosting.END_OF_LIST) {
			docids.add(p.getId());
		}
		p.close();
		
		for (int i = 1; i < terms.length; i++) {
			le = originalIndex.getLexicon().getLexiconEntry(terms[i]);
			p = originalIndex.getInvertedIndex().getPostings(le);
		
			while (p.next() != IterablePosting.END_OF_LIST) {
				if (!docids.contains(p.getId()))
					docids.add(p.getId());
			}
			p.close();
		
			Collections.sort(docids);
		}
		System.err.println("Union of terms \"" + query + "\" contains " + docids.size() + " documents");
	}
		
	@Test public void originalOr() throws IOException
	{
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		getOrCorrectResults();
		
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.Or");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new BooleanManager(originalIndex);
		ResultSet rs = manager.run(srq);
		manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		for (int i = 0; i < andDocids.length; i++)
			assertEquals(andDocids[i], docids.getInt(i));
	}
	
	@Test public void eliasFanoOr() throws IOException
	{
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		getOrCorrectResults();
		
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.Or");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new BooleanManager(efIndex);
		ResultSet rs = manager.run(srq);
		manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		for (int i = 0; i < andDocids.length; i++)
			assertEquals(andDocids[i], docids.getInt(i));
	}
}