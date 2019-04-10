package it.cnr.isti.hpclab.matching;

import it.cnr.isti.hpclab.annotations.Managed;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.manager.WandManager;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import java.util.Collections;

import org.terrier.structures.postings.IterablePosting;

@Managed(by = "it.cnr.isti.hpclab.manager.WandManager")
public class Wand implements MatchingAlgorithm
{		
	private WandManager manager;
	
	@Override
	public void setup(final Manager manager) 
	{		
		this.manager = (WandManager) manager;
	}

	@Override
	public long match(final int from, final int to) throws IOException 
	{
		final ObjectList<WandManager.Tuple> enums = manager.enums;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;

		manager.reset_to(from);
		Collections.sort(enums);
		
		long start_time = System.nanoTime();
		while (true) {
			// find pivot
			float upper_bound = 0;
            int pivot;
            boolean found_pivot = false;
            for (pivot = 0; pivot < enums.size(); ++pivot) {
                if (enums.get(pivot).posting.getId() >= to) {
                    break;
                }
                upper_bound += enums.get(pivot).maxscore;
                if (heap.wouldEnter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }

            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }
            
            // check if pivot is a possible match
            int pivot_id = enums.get(pivot).posting.getId();
            manager.numPivots++;
            
            if (pivot_id == enums.get(0).posting.getId()) {
            	manager.partiallyProcessedDocuments++;
                float score = 0;
                for (int i = 0; i < enums.size(); i++) {
                	IterablePosting p = enums.get(i).posting;
                	if (p.getId() != pivot_id) {
                		break;
                	}
        			score += wm.score(1, p, enums.get(i).entry);
        			manager.processedPostings += 1;
        			p.next();
                }
                heap.insert(new Result(pivot_id, score));
                // resort by docid
                Collections.sort(enums);
            } else {
                // no match, move farthest list up to the pivot
                int next_list = pivot;
                for (; enums.get(next_list).posting.getId() == pivot_id; --next_list)
                	;
                enums.get(next_list).posting.next(pivot_id);
                // bubble down the advanced list
                for (int i = next_list + 1; i < enums.size(); ++i) {
                	if (enums.get(i).posting.getId() < enums.get(i-1).posting.getId()) {
                		Collections.swap(enums, i, i - 1);
                	} else {
                		break;
                	}
                }
            }
		}
		return System.nanoTime() - start_time;
	}
}
