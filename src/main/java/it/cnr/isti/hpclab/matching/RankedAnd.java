package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.RankedManager;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;

@Managed(by = "it.cnr.isti.hpclab.manager.RankedManager")
public class RankedAnd implements MatchingAlgorithm
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
		final ObjectList<RankedManager.Tuple> enums = manager.enums;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;
		
		manager.reset_to(from);
		
		long start_time = System.nanoTime();
        int currentDocid = enums.get(0).posting.getId();
        float currentScore = 0.0f;
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
        		currentScore = 0.0f;
        		for (int j = 0; j < enums.size(); j++) {
        			currentScore += wm.score(1, enums.get(j).posting, enums.get(j).entry);
        		}
        		manager.processedPostings += enums.size();
        		heap.insert(new Result(currentDocid, currentScore));

        		currentDocid = enums.get(0).posting.next();
        		i = 1;
        	}
        }
        return System.nanoTime() - start_time;
	}	
}
