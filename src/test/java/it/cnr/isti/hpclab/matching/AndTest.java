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
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;

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

/*
 Some useful data to generated good queries in the provided test collection:
 
	Intersection of terms "1 1996" 		 contains 578 documents
	Intersection of terms "1996 mail" 	 contains 503 documents
	Intersection of terms "1996 new" 	 contains 603 documents
	Intersection of terms "home 1996" 	 contains 512 documents
	Intersection of terms "home page" 	 contains 512 documents
	Intersection of terms "includ 1996"  contains 525 documents
	Intersection of terms "internet new" contains 507 documents
	Intersection of terms "link 1996" 	 contains 508 documents
	Intersection of terms "new 1996" 	 contains 745 documents
	Intersection of terms "page 1996" 	 contains 671 documents
	Intersection of terms "page mail" 	 contains 551 documents
	Intersection of terms "page new" 	 contains 603 documents
	Intersection of terms "site 1996" 	 contains 598 documents
	Intersection of terms "web 1996" 	 contains 534 documents
	Intersection of terms "will new" 	 contains 502 documents
	Intersection of terms "world 1996" 	 contains 525 documents
 */

@RunWith(value = Parameterized.class)
public class AndTest extends MatchingSetupTest
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
		// return Arrays.asList(new Object[][] { {query3} });
	}
	
	public AndTest(String query)
	{
		this.query = query;
	}

	@BeforeClass public static void init() throws Exception
	{
		MatchingSetupTest.makeEnvironment();
		MatchingSetupTest.doWT10GSampleIndexing();
		
		/*
		String args[] = new String[2];
		args[0] = terrierEtc;
		args[1] = "data";
		it.cnr.isti.hpclab.ef.Generator.LOG2QUANTUM = 3;
		it.cnr.isti.hpclab.ef.Generator.main(args);
		*/
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

	private void getAndCorrectResults() throws IOException
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
			IntList docids2 = new IntArrayList();
			p = originalIndex.getInvertedIndex().getPostings(le);
		
			while (p.next() != IterablePosting.END_OF_LIST) {
				docids2.add(p.getId());
			}
			p.close();
		
			docids.retainAll(docids2); // docids1 contains the intersection
		}
		System.err.println("Intersection of terms \"" + query + "\" contains " + docids.size() + " documents");
	}
	
	@Test public void originalAnd() throws IOException
	{
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		getAndCorrectResults();
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.And");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");
		openIndex();
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new BooleanManager(originalIndex);
		ResultSet rs = manager.run(srq);
		manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		for (int i = 0; i < andDocids.length; i++)
			assertEquals(andDocids[i], docids.getInt(i));
	}
	
	@Test public void eliasFanoAnd() throws IOException
	{
		MatchingConfiguration.set(Property.TOP_K, "100000000");
		getAndCorrectResults();
	
		MatchingConfiguration.set(Property.IGNORE_LOW_IDF_TERMS, "false");
		MatchingConfiguration.set(Property.MATCHING_ALGORITHM_CLASSNAME, "it.cnr.isti.hpclab.matching.And");
		MatchingConfiguration.set(Property.WEIGHTING_MODEL_CLASSNAME,    "it.cnr.isti.hpclab.matching.structures.model.BM25");
		openIndex();
		SearchRequest srq = new SearchRequest(1, query);
		
		Manager manager = new BooleanManager(efIndex);
		ResultSet rs = manager.run(srq);
		manager.close();
		
		int[] andDocids = rs.docids();
		
		assertEquals(andDocids.length, docids.size());

		for (int i = 0; i < andDocids.length; i++)
			assertEquals(andDocids[i], docids.getInt(i));
	}	

	public static void main(String[] args) throws Exception
	{
		MatchingSetupTest.makeEnvironment();
		MatchingSetupTest.doWT10GSampleIndexing();

		IntList docids = new IntArrayList();
		List<String> good_terms = new ObjectArrayList<String>();
		// List<String> good_bigrams = new ObjectArrayList<String>();
		
		Index originalIndex = Index.createIndex();
		int num_terms = originalIndex.getCollectionStatistics().getNumberOfUniqueTerms();
		for (int i = 0 ; i < num_terms; i++) {
			Entry<String, LexiconEntry> e = originalIndex.getLexicon().getIthLexiconEntry(i);
			if (e.getValue().getDocumentFrequency() >= 30) {
				// System.err.println(e.getKey() + "\t" + e.getValue());
				good_terms.add(e.getKey());
			}
		}

		for (String t1: good_terms) {
			for (String t2: good_terms) {
				if (!t1.equals(t2)) {
					String[] terms = {t1, t2};
					LexiconEntry le = originalIndex.getLexicon().getLexiconEntry(terms[0]);
					IterablePosting p = originalIndex.getInvertedIndex().getPostings(le);
					
					while (p.next() != IterablePosting.END_OF_LIST) {
						docids.add(p.getId());
					}
					p.close();
					
					for (int i = 1; i < terms.length; i++) {
						le = originalIndex.getLexicon().getLexiconEntry(terms[i]);
						IntList docids2 = new IntArrayList();
						p = originalIndex.getInvertedIndex().getPostings(le);
					
						while (p.next() != IterablePosting.END_OF_LIST) {
							docids2.add(p.getId());
						}
						p.close();
					
						docids.retainAll(docids2); // docids1 contains the intersection
					}
					if (docids.size() > 500) {
						System.err.println("Intersection of terms \"" + t1 + " " + t2 + "\" contains " + docids.size() + " documents");
					}
				}
			}
		}
		
		MatchingSetupTest.deleteTerrierEtc();
	}
}
