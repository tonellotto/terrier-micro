package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.MaxScoreManager;
import it.cnr.isti.hpclab.matching.structures.Result;

import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;

import org.terrier.structures.postings.IterablePosting;

@Managed(by = "it.cnr.isti.hpclab.manager.MaxScoreManager")
public class MaxScore implements MatchingAlgorithm
{
	private MaxScoreManager manager;
	
	@Override
	public void setup(final Manager manager) 
	{		
		this.manager = (MaxScoreManager) manager;
	}

	@Override
	public long match(final int from, final int to) throws IOException 
	{
		final ObjectList<MaxScoreManager.Tuple> ordered_enums = manager.ordered_enums;
		final float upper_bounds[] = manager.upper_bounds;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;
		
		manager.reset_to(from);
		
		int non_essential_lists = 0;
		
		// The following cycle is run only if initial threshold is greater than 0.
		while (non_essential_lists < ordered_enums.size() && !heap.wouldEnter(upper_bounds[non_essential_lists]))
			non_essential_lists += 1;
		
		long start_time = System.nanoTime();
		int currentDocid = manager.min_docid();
		int nextDocid;
		
		IterablePosting p;
        while (non_essential_lists < ordered_enums.size() && currentDocid < to) 
        {
        	Result result = new Result(currentDocid);
        	nextDocid = Integer.MAX_VALUE;

        	// We compute the score of the essential lists
        	for (int i = non_essential_lists; i < ordered_enums.size(); ++i) {
        		p = ordered_enums.get(i).posting;
        		if (p.getId() == currentDocid) {
        			result.updateScore(wm.score(1, p, ordered_enums.get(i).entry));
        			manager.processedPostings += 1;
        			p.next();
        		}
        		
        		if (p.getId() < nextDocid) {
        			nextDocid = p.getId();
        		}	
        	}

        	manager.partiallyProcessedDocuments++;
        	
        	// We try to complete evaluation with non-essential lists
        	for (int i = non_essential_lists - 1; i + 1 > 0; --i) {
        		if (!heap.wouldEnter(result.getScore() + upper_bounds[i]))
                    break;

        		p = ordered_enums.get(i).posting;
        		p.next(currentDocid);

        		if (p.getId() == currentDocid) {
        			result.updateScore(wm.score(1, p, ordered_enums.get(i).entry));
        			manager.processedPostings += 1;
        		}
        	}
       	
        	if (heap.insert(result)) {
        		int old = non_essential_lists;
        		// update non-essential lists
        		while (non_essential_lists < ordered_enums.size() && !heap.wouldEnter(upper_bounds[non_essential_lists])) {
        			non_essential_lists += 1;
        		}
        		if (old != non_essential_lists)
        			manager.numPivots++;
        	}
        	currentDocid = nextDocid;
        }
        return System.nanoTime() - start_time;
	}	
}
