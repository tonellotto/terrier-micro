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

package it.cnr.isti.hpclab.manager;

import java.io.IOException;
import java.util.Arrays;

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
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

public class BlockMaxWandManager extends Manager 
{
	public Tuple[] enums;
	public TopQueue heap;
	public float threshold;
	
	public BlockMaxWandManager()
	{
		super();
	}
	
	public BlockMaxWandManager(final Index index)
	{
		super(index);
	}

	@Override
	public ResultSet run(SearchRequest srq) throws IOException 
	{
		look(srq);
		if (enums.length == 0)
			return new EmptyResultSet();
		
		processedPostings = 0l;
		partiallyProcessedDocuments = 0l;
		numPivots = 0l;
		
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
		srq.getQuery().addMetadata(RuntimeProperty.QUERY_TERMS, Arrays.toString(Arrays.stream(enums).map(x -> "\"" + x.term + "\"").collect(Collectors.toList()).toArray()));
		
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSING_TIME, Double.toString(processingTime/1e6));
        srq.getQuery().addMetadata(RuntimeProperty.QUERY_LENGTH,    Integer.toString(enums.length));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_POSTINGS, Long.toString(processedPostings));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_DF, Arrays.toString(Arrays.stream(enums).map(x -> x.entry.getDocumentFrequency()).collect(Collectors.toList()).toArray()));
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_MS, Arrays.toString(Arrays.stream(enums).map(x -> x.maxscore).collect(Collectors.toList()).toArray()));
        srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(heap.threshold()));
        srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(threshold));
        srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, Integer.toString(heap.size()));
        
        srq.getQuery().addMetadata(RuntimeProperty.PARTIALLY_PROCESSED_DOCUMENTS, Long.toString(partiallyProcessedDocuments));
        srq.getQuery().addMetadata(RuntimeProperty.NUM_PIVOTS, Long.toString(numPivots));
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

	protected void look(final SearchRequest searchRequest) throws IOException
	{
		MaxScoreIndex maxScoreIndex = (MaxScoreIndex) mIndex.getIndexStructure("maxscore");
		BlockMaxScoreIndex blockMaxScoreIndex = (BlockMaxScoreIndex) mIndex.getIndexStructure("blockmaxscore");
				
		final int num_docs = mIndex.getCollectionStatistics().getNumberOfDocuments();
		ObjectList<Tuple> enums_with_maxscore = new ObjectArrayList<Tuple>();
		
		
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
				enums_with_maxscore.add(new Tuple(term, ip, le, maxScoreIndex.getMaxScore(le.getTermId()), blockMaxScoreIndex.get(le.getTermId()), Tuple.SORT_BY_DOCID));
			}
		}
		
		maxScoreIndex.close();	
		
		enums = enums_with_maxscore.toArray(new Tuple[0]);
	}
	
	@Override
	public void reset_to(final int to) throws IOException
	{
		for (Tuple t: enums) {
			t.posting.close();
			t.posting =  mIndex.getInvertedIndex().getPostings(t.entry);
			t.posting.next();
			t.posting.next(to);
			
			t.blockEnum.reset();
			t.blockEnum.move(to);
		}
	}
}