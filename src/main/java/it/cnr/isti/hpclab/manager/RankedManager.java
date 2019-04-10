package it.cnr.isti.hpclab.manager;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.Query.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class RankedManager extends Manager 
{
	public ObjectList<Tuple> enums;
	public TopQueue heap;
	public float threshold;
	
	public RankedManager()
	{
		super();
	}
	
	public RankedManager(final Index index)
	{
		super(index);
	}
	
	@Override
	public ResultSet run(final SearchRequest srq) throws IOException 
	{	
		look(srq);		
		if (enums.size() == 0)
			return new EmptyResultSet();
		Collections.sort(enums);
	
		processedPostings = 0l;	
		threshold = parseFloat(srq.getQuery().getMetadata(RuntimeProperty.INITIAL_THRESHOLD));
		heap = new TopQueue(TOP_K(), threshold);
		
		processingTime = mMatchingAlgorithm.match();

		close_enums();
		stats_enums(srq);
		
        if (heap.isEmpty())
        	return new EmptyResultSet();
 							
       	return new ScoredResultSet(heap);
	}
	
	protected void stats_enums(final SearchRequest srq)
	{
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_TERMS, Arrays.toString(enums.stream().map(x -> "\"" + x.term + "\"").collect(Collectors.toList()).toArray()));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSING_TIME, Double.toString(processingTime/1e6));
        srq.getQuery().addMetadata(RuntimeProperty.QUERY_LENGTH,    Integer.toString(enums.size()));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_POSTINGS, Long.toString(processedPostings));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_DF, Arrays.toString(enums.stream().map(x -> x.entry.getDocumentFrequency()).collect(Collectors.toList()).toArray()));
        srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(heap.threshold()));
        srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(threshold));        
	}

	public static float parseFloat(String s)
	{
		if (s == null)
			return 0.0f;
		return Float.parseFloat(s);
	}

	protected void close_enums() throws IOException
	{
		for (Tuple pair: enums)
        	pair.posting.close();
	}
	
	public final int min_docid() 
	{
		int docid = Integer.MAX_VALUE;
		for (int i = 0; i < enums.size(); i++)
			if (enums.get(i).posting.getId() < docid)
				docid = enums.get(i).posting.getId();
		return docid;
	}

	public static class Tuple implements Comparable<Tuple>
	{
		public String term = null;
		public IterablePosting posting = null;
		public LexiconEntry entry = null;
		
		public Tuple(final String term, final IterablePosting posting, final LexiconEntry entry)
		{
			this.term = term;
			this.posting = posting;
			this.entry = entry;
		}
		
		@Override
		public String toString()
		{
			return posting.toString() + ", [" + entry.getDocumentFrequency() + "," + entry.getFrequency() + "]"; 
		}
				
		@Override
		public int compareTo(Tuple that) 
		{
			return Integer.compare(this.entry.getDocumentFrequency(), that.entry.getDocumentFrequency());
		}
	}
	
	protected void look(final SearchRequest searchRequest) throws IOException
	{	
		enums = new ObjectArrayList<Tuple>();
		
		final int num_docs = mIndex.getCollectionStatistics().getNumberOfDocuments();
		
		// We look in the index and filter out common terms
		for (String term: searchRequest.getQueryTerms()) {
			LexiconEntry le = mIndex.getLexicon().getLexiconEntry(term);
			if (le == null) {
				LOGGER.warn("Term not found in index: " + term);
			} else if (IGNORE_LOW_IDF_TERMS && le.getFrequency() > num_docs) {
				LOGGER.warn("Term " + term + " has low idf - ignored from scoring.");
			} else {
				IterablePosting ip = mIndex.getInvertedIndex().getPostings(le);
				ip.next();
				enums.add(new Tuple(term, ip, le));
			}
		}		
	}
	
	@Override
	public void reset_to(final int to) throws IOException
	{
		for (Tuple t: enums) {
			t.posting.close();
			t.posting =  mIndex.getInvertedIndex().getPostings(t.entry);
			t.posting.next();
			t.posting.next(to);
		}
	}
}
