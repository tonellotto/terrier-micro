package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.BooleanManager;
import it.cnr.isti.hpclab.manager.Manager;

import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;

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
		final ObjectList<BooleanManager.Tuple> enums = manager.enums;
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
