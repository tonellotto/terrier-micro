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
import it.cnr.isti.hpclab.manager.BlockMaxWandManager;
import it.cnr.isti.hpclab.manager.Manager;
import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import it.cnr.isti.hpclab.maxscore.structures.BlockEnumerator;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;

@Managed(by = "it.cnr.isti.hpclab.manager.BlockMaxWandManager")
public class BlockMaxWand implements MatchingAlgorithm
{		
	private BlockMaxWandManager manager;
	
	@Override
	public void setup(final Manager manager) 
	{		
		this.manager = (BlockMaxWandManager) manager;
	}

	@Override
	public long match(final int from, final int to) throws IOException 
	{		
		BlockMaxWandManager.Tuple[] enums = manager.enums;
		final TopQueue heap = manager.heap;
		final WeightingModel wm = manager.mWeightingModel;

		manager.reset_to(from);
		Arrays.sort(enums);
		
		long start_time = System.nanoTime();
		while (true) {
			// find pivot
			float upper_bound = 0;
            int pivot;
            boolean found_pivot = false;
            for (pivot = 0; pivot < enums.length; ++pivot) {
                if (enums[pivot].posting.getId() >= to) {
                    break;
                }

                upper_bound += enums[pivot].maxscore;
                if (heap.wouldEnter(upper_bound)) {
                    found_pivot = true;
                    break;
                }
            }
            
            // no pivot found, we can stop the search
            if (!found_pivot) {
                break;
            }
            
            manager.numPivots++;
            
            int pivot_id = enums[pivot].posting.getId();

            while (pivot < enums.length - 1 && enums[pivot + 1].posting.getId() == pivot_id)
            	pivot++;
            
            float blockMaxScore = 0.0f;
            // long next_b = Long.MAX_VALUE;
            
            // shallow move
            for (int i = 0; i <= pivot; ++i) {
            	BlockEnumerator b = enums[i].blockEnum;
            	b.move(pivot_id);
            	blockMaxScore += b.score();
            	// We do not compute next_b here since it's faster to do it later on... (check comments)
            	//
            	// if (b.last() < next_b)
            	// 	next_b = b.last();
            	// num_shallow_moves++;
            	//
            }
            
            if (heap.wouldEnter(blockMaxScore)) {
            	if (pivot_id == enums[0].posting.getId()) {
            		manager.partiallyProcessedDocuments++;
            		float score = 0.0f;
            		float p_score = 0.0f;
            		for (int i = 0; i <= pivot; ++i) {
            			score += (p_score = wm.score(1, enums[i].posting, enums[i].entry));
            			manager.processedPostings++;
            			// Early termination
            			blockMaxScore -= (enums[i].blockEnum.score() - p_score);
            			if (!heap.wouldEnter(blockMaxScore))
            				break;
            		}
            		for (int i = 0; i <= pivot; ++i) 
            			enums[i].posting.next();
            		heap.insert(new Result(pivot_id, score));
            		Arrays.sort(enums);
            	} else {
                    // no match, move farthest list up to the pivot
                    int next_list = pivot;
                    for (; enums[next_list].posting.getId() == pivot_id; --next_list)
                    	;
                    // Deep move
                    enums[next_list].posting.next(pivot_id);
                    // bubble down the advanced list
                    for (int i = next_list + 1; i < enums.length; ++i) {
                    	if (enums[i].posting.getId() < enums[i-1].posting.getId()) {
                    		ArrayUtils.swap(enums, i, i - 1);
                    	} else {
                    		break;
                    	}
                    }
            	}
            } else {
            	// Faster way to select next_b
            	long next_b = Long.MAX_VALUE;
                for (int i = 0; i <= pivot; ++i) {
                	BlockEnumerator b = enums[i].blockEnum;
                	if (b.last() < next_b)
                		next_b = b.last();
                }

            	if (pivot < enums.length - 1 && next_b > enums[pivot + 1].posting.getId())
            		next_b = enums[pivot + 1].posting.getId();
            	if (next_b <= pivot_id)
            		next_b++;
                int next_list = pivot;
                // Nasty bug: dobbiamo controllare che funzioni anche con una sola posting list, quindi ci vuole next_list > 0 
                // Perchè potremmo essere saltati qui con un buon docid ma con block maxscore basso...
                for (; enums[next_list].posting.getId() == pivot_id && next_list > 0; --next_list)
                	;
                // Deep move
                enums[next_list].posting.next((int) next_b);
                // bubble down the advanced list
                for (int i = next_list + 1; i < enums.length; ++i) {
                	if (enums[i].posting.getId() < enums[i-1].posting.getId()) {
                		ArrayUtils.swap(enums, i, i - 1);
                	} else {
                		break;
                	}
                }            	
            }
		}
		return System.nanoTime() - start_time;
	}
}