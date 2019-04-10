package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.RankedManager;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;

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
		final ObjectList<RankedManager.Tuple> enums = manager.enums;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;

		manager.reset_to(from);

		long start_time = System.nanoTime();
		int currentDocid = manager.min_docid();
		float currentScore = 0.0f;
        while (currentDocid < to) {
        	int nextDocid = Integer.MAX_VALUE;
        	for (int i = 0; i < enums.size(); i++) {
        		IterablePosting p = enums.get(i).posting;
        		if (p.getId() == currentDocid) {
        			currentScore += wm.score(1, p, enums.get(i).entry);
        			manager.processedPostings += 1;
        			p.next();
        		}
        		if (p.getId() < nextDocid)
        			nextDocid = p.getId();
        	}
        	heap.insert(new Result(currentDocid, currentScore));
        	currentDocid = nextDocid;
        	currentScore = 0.0f;
        }
        return System.nanoTime() - start_time;
	}
}
