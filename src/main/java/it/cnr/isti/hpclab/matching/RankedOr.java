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

package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.cnr.isti.hpclab.manager.RankedManager;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import java.io.IOException;
import java.util.List;

import org.terrier.structures.postings.IterablePosting;

@Managed(by = "it.cnr.isti.hpclab.manager.RankedManager")
public class RankedOr implements MatchingAlgorithm
{		
	private RankedManager manager;
	
	@Override
	public void setup(final Manager manager) 
	{		
		this.manager = (RankedManager) manager;
	}

	@Override
	public long match(int from, int to) throws IOException 
	{
		final List<MatchingEntry> enums = manager.enums;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;

		manager.reset_to(from);

		long start_time = System.nanoTime();
		int currentDocid = manager.min_docid();
		float currentScore = 0.0f;
        while (currentDocid < to) {
        	int nextDocid = Integer.MAX_VALUE;
        	int numRequired = 0;
        	for (int i = 0; i < enums.size(); i++) {
        		IterablePosting p = enums.get(i).posting;
        		if (p.getId() == currentDocid) {
        			currentScore += wm.score(enums.get(i).qtf, p, enums.get(i).entry) * enums.get(i).weight;
        			manager.processedPostings += 1;
        			p.next();
        			if (enums.get(i).term.isRequired())
        				numRequired++;
        		}
        		if (p.getId() < nextDocid)
        			nextDocid = p.getId();
        	}
        	if (numRequired == manager.numRequired)
        		heap.insert(new Result(currentDocid, currentScore));
        	
        	currentDocid = nextDocid;
        	currentScore = 0.0f;
        }
        return System.nanoTime() - start_time;
	}
}
