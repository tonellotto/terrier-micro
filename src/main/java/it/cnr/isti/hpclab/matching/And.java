package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.BooleanManager;
import it.cnr.isti.hpclab.manager.Manager;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;

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
		final ObjectList<BooleanManager.Tuple> enums = manager.enums;
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
