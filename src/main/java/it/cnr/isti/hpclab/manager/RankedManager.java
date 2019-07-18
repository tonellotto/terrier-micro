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
import java.util.Collections;

import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.annotations.Managing;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.SearchRequest;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.Query.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.ScoredResultSet;

@Managing(algorithms = "it.cnr.isti.hpclab.matching.RankedAnd,it.cnr.isti.hpclab.matching.RankedOr")
public class RankedManager extends Manager 
{
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
		open_enums(srq);
		
		if (enums.size() == 0)
			return new EmptyResultSet();
		
		Collections.sort(enums, MatchingEntry.SORT_BY_DOCID);
	
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
	
	@Override
	protected void stats_enums(final SearchRequest srq)
	{
		super.stats_enums(srq);
		
        srq.getQuery().addMetadata(RuntimeProperty.FINAL_THRESHOLD,    Float.toString(heap.threshold()));
        srq.getQuery().addMetadata(RuntimeProperty.INITIAL_THRESHOLD,  Float.toString(threshold));
        srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, Integer.toString(heap.size()));
	}

	public static float parseFloat(String s)
	{
		if (s == null)
			return 0.0f;
		return Float.parseFloat(s);
	}

	@Override
	protected MatchingEntry entryFrom(final String term, final IterablePosting posting, final LexiconEntry entry) throws IOException
	{
		return new MatchingEntry(term, posting, entry);
	}	
}
