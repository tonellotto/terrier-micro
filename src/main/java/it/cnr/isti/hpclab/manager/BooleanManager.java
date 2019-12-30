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
import it.cnr.isti.hpclab.matching.structures.query.QueryTerm;
import it.cnr.isti.hpclab.matching.structures.QueryProperties.RuntimeProperty;
import it.cnr.isti.hpclab.matching.structures.resultset.DocidResultSet;
import it.cnr.isti.hpclab.matching.structures.resultset.EmptyResultSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

@Managing(algorithms = "it.cnr.isti.hpclab.matching.And,it.cnr.isti.hpclab.matching.Or")
public class BooleanManager extends Manager
{
	public IntList docids;		
	
	public BooleanManager()
	{
		super();
	}
	
	public BooleanManager(final Index index)
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
		docids = new IntArrayList();
		
		processingTime = mMatchingAlgorithm.match();
		
		close_enums();
		stats_enums(srq);
		
        if (docids.size() > TOP_K())
        	docids.removeElements(TOP_K() + 1, docids.size()); // Can be safely removed if we do not need results to return
        
        if (docids.size() != 0)  			   // Can be safely removed if we do not need results to return
        	return new DocidResultSet(docids); // Can be safely removed if we do not need results to return

       	return new EmptyResultSet();	   // Can be safely removed if we do not need results to return        
	}
	
	@Override
	protected void stats_enums(final SearchRequest srq)
	{
		super.stats_enums(srq);
		
        if (docids.size() > TOP_K())
        	srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, Integer.toString(TOP_K()));
        else 
        	srq.getQuery().addMetadata(RuntimeProperty.NUM_RESULTS, Integer.toString(docids.size()));
	}
	
	@Override
	protected MatchingEntry entryFrom(final int qtf, final QueryTerm term, final IterablePosting posting, final LexiconEntry entry) throws IOException
	{
		return new MatchingEntry(qtf, term, posting, entry);
	}	
}