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

import it.cnr.isti.hpclab.annotations.Managing;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;
import it.cnr.isti.hpclab.maxscore.structures.BlockEnumerator;
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;

@Managing(algorithms = "it.cnr.isti.hpclab.matching.And,it.cnr.isti.hpclab.matching.BlockMaxWand")
public class BlockMaxWandManager extends WandManager 
{
	public MatchingEntry[] local_enums;
	
	/*
	public BlockMaxWandManager()
	{
		super();
	}
	*/
	
	public BlockMaxWandManager(final Index index)
	{
		super(index);
	}

	@Override
	public ResultSet run(SearchRequest srq) throws IOException 
	{
		open_enums(srq);
		
		if (local_enums.length == 0)
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

	@Override
	protected void stats_enums(final SearchRequest srq)
	{
		super.stats_enums(srq);
		
        srq.getQuery().addMetadata(RuntimeProperty.PROCESSED_TERMS_MS, Arrays.toString(Arrays.stream(local_enums).map(x -> x.maxscore).collect(Collectors.toList()).toArray()));
        
        srq.getQuery().addMetadata(RuntimeProperty.PARTIALLY_PROCESSED_DOCUMENTS, Long.toString(partiallyProcessedDocuments));
        srq.getQuery().addMetadata(RuntimeProperty.NUM_PIVOTS, Long.toString(numPivots));
	}

	@Override
	protected MatchingEntry entryFrom(final int qtf, final QueryTerm term, final IterablePosting posting, final LexiconEntry entry) throws IOException
	{
		MaxScoreIndex maxScoreIndex = (MaxScoreIndex) mIndex.getIndexStructure("maxscore");
		float ms = maxScoreIndex.getMaxScore(entry.getTermId());
		maxScoreIndex.close();
		
		BlockMaxScoreIndex blockMaxScoreIndex = (BlockMaxScoreIndex) mIndex.getIndexStructure("blockmaxscore");
		BlockEnumerator be = blockMaxScoreIndex.get(entry.getTermId(), term.getWeight());
		return new MatchingEntry(qtf, term, posting, entry, ms, be);
	}	

	@Override
	protected void open_enums(final SearchRequest searchRequest) throws IOException
	{
		super.open_enums(searchRequest);
		
		local_enums = enums.toArray(new MatchingEntry[0]);
	}

	@Override
	public void reset_to(final int to) throws IOException
	{
		for (MatchingEntry t: local_enums) {
			t.posting.close();
			t.posting =  mIndex.getInvertedIndex().getPostings(t.entry);
			t.posting.next();
			t.posting.next(to);

			t.blockEnum.reset();
			t.blockEnum.move(to);
		}
	}
}