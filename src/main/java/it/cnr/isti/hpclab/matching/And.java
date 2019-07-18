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
public class And implements MatchingAlgorithm
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
        int currentDocid = enums.get(0).posting.getId();
        int i = 1;
        while (currentDocid < to) {
        	for (; i < enums.size(); i++) {
        		enums.get(i).posting.next(currentDocid);
        		if (enums.get(i).posting.getId() != currentDocid) {
        			currentDocid = enums.get(i).posting.getId();
        			i = 0;
        			break;
        		}
        	}
        	
        	if (i == enums.size()) {
        		docids.add(enums.get(0).posting.getId()); // Can be safely removed if we do not need results to return
        		manager.processedPostings += enums.size();
        		currentDocid = enums.get(0).posting.next();
        		i = 1;
        	}
        }
        return System.nanoTime() - start_time;
	}	
}
