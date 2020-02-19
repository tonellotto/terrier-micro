package it.cnr.isti.hpclab.matching.structures.query;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import it.cnr.isti.hpclab.MatchingConfiguration;
import it.cnr.isti.hpclab.MatchingConfiguration.Property;

public class TestTrecQuerySource 
{
	private static final int[] correct_qids = {301, 302, 699};
	private static final String[] correct_queries = {"international organized crime", "poliomyelitis and post polio", "term limits"};
	@ClassRule
	public static TemporaryFolder tmpFolder = null;
	public static String tmpFolderName = null;

	private void createTrecTopicsFile() throws IOException
	{
		tmpFolder = new TemporaryFolder();
		tmpFolder.create();
		tmpFolderName = tmpFolder.getRoot().toString();
		
		PrintWriter file = new PrintWriter(new FileWriter(tmpFolderName + File.separator + "trec.topics.txt"));

		file.println("");
		file.println("<top>");
		file.println("");
		file.println("<num> Number: 301"); 
		file.println("<title> International Organized Crime"); 
		file.println("");
		file.println("<desc> Description:"); 
		file.println("Identify organizations that participate in international criminal");
		file.println("activity, the activity, and, if possible, collaborating organizations");
		file.println("and the countries involved.");
		file.println("");
		file.println("<narr> Narrative:"); 
		file.println("A relevant document must as a minimum identify the organization and the");
		file.println("type of illegal activity (e.g., Columbian cartel exporting cocaine).");
		file.println("Vague references to international drug trade without identification of");
		file.println("the organization(s) involved would not be relevant.");
		file.println("");
		file.println("</top>");
		file.println("");
		file.println("");
		file.println("<top>");
		file.println("");
		file.println("<num> Number: 302");
		file.println("<title> Poliomyelitis and Post-Polio"); 
		file.println("");
		file.println("<desc> Description:"); 
		file.println("Is the disease of Poliomyelitis (polio) under control in the");
		file.println("world?");
		file.println("");
		file.println("<narr> Narrative:"); 
		file.println("Relevant documents should contain data or outbreaks of the"); 
		file.println("polio disease (large or small scale), medical protection ");
		file.println("against the disease, reports on what has been labeled as ");
		file.println("\"post-polio\" problems.  Of interest would be location of ");
		file.println("the cases, how severe, as well as what is being done in ");
		file.println("the \"post-polio\" area.");
		file.println("");
		file.println("</top>");
		file.println("");
		file.println("<top>");
		file.println("<num> Number: 699");
		file.println("");
		file.println("<title>");
		file.println("term limits"); 
		file.println("");
		file.println("<desc>");
		file.println("What are the pros and cons of term limits?");
		file.println("");
		file.println("<narr>");
		file.println("Relevant documents reflect an opinion on the value of term limits");
		file.println("with accompanying reason(s).  Documents that cite the status of term");
		file.println("limit legislation or opinions on the issue sans reasons for the opinion");
		file.println("are not relevant.");
		file.println("</top>");
		file.println("");

		file.println("");
		file.println("");
		file.println("");
		file.println("");
		file.println("");
		file.println("");
		file.println("");
		
		file.close();
	}
	
	@Test
	public void test() throws IOException
	{
		createTrecTopicsFile();
		MatchingConfiguration.set(Property.MUST_TOKENISE, "true");
		
		QuerySource tqs = new TrecQuerySource(tmpFolderName + File.separator + "trec.topics.txt");
		int cnt = 0;
		while (tqs.hasNext()) {
			String query = tqs.next();
			int qid = tqs.getQueryId();
			assertEquals(qid, correct_qids[cnt]);
			assertEquals(query, correct_queries[cnt]);
			cnt++;
		}	
		tmpFolder.delete();
	}
}
