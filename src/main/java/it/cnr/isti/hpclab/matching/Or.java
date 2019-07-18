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
import it.cnr.isti.hpclab.manager.BooleanManager;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.MatchingEntry;
import it.unimi.dsi.fastutil.ints.IntList;

import java.io.IOException;
import java.util.List;

@Managed(by = "it.cnr.isti.hpclab.manager.BooleanManager")
public class Or implements MatchingAlgorithm
{
	private BooleanManager manager;
	
	@Override
	public void setup(final Manager manager) 
	{		
		this.manager = (BooleanManager) manager;
	}
		
	@Override
	public long match(int from, int to) throws IOException 
	{
		final List<MatchingEntry> enums = manager.enums;
		final IntList docids = manager.docids; // Can be safely removed if we do not need results to return
		
		manager.reset_to(from);
		
		long start_time = System.nanoTime();
		int currentDocid = manager.min_docid();
        while (currentDocid < to) {
        	docids.add(currentDocid); // Can be safely removed if we do not need results to return
        	int nextDocid = Integer.MAX_VALUE;
        	for (int i = 0; i < enums.size(); i++) {
        		if (enums.get(i).posting.getId() == currentDocid) {
        			enums.get(i).posting.next();
                	manager.processedPostings += 1;
        		}
        		if (enums.get(i).posting.getId() < nextDocid)
        			nextDocid = enums.get(i).posting.getId();
        	}
        	currentDocid = nextDocid;
        }
        return System.nanoTime() - start_time;
	}
}
