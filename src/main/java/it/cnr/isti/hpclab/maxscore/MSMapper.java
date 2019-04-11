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

package it.cnr.isti.hpclab.maxscore;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terrier.structures.BitIndexPointer;
import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.ef.TermPartition;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;

class MSMapper implements Function<TermPartition,Object>
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(MSMapper.class);
	
	private final String src_index_path;
	private final String src_index_prefix;
	private final String wm_name;
	
	// shared array
	private final float[] msa;
	
	public MSMapper(final String src_index_path, final String src_index_prefix, final String wm_name, final float[] msa) 
	{
		this.src_index_path = src_index_path;
		this.src_index_prefix = src_index_prefix;
		this.wm_name = wm_name;
		this.msa = msa;		
	}

	@Override
	public Object apply(TermPartition terms) 
	{		
		try {
			Index src_index = Index.createIndex(src_index_path, src_index_prefix);
			
			if (terms.begin >= terms.end || terms.begin < 0 || terms.end > src_index.getCollectionStatistics().getNumberOfUniqueTerms()) {
				LOGGER.error("Something wrong with termids, begin = " + terms.begin + ", end = " + terms.end);
				return null;
			}

			WeightingModel wm_model = (WeightingModel) (Class.forName(wm_name).asSubclass(WeightingModel.class).getConstructor().newInstance());
			wm_model.setup(src_index);
			
			// opening src index lexicon iterator and moving to the begin termid
			Iterator<Entry<String, LexiconEntry>> lex_iter = src_index.getLexicon().iterator();
			Entry<String, LexiconEntry> lee = null;
			
			while (lex_iter.hasNext()) {
				lee = lex_iter.next();
				if (lee.getValue().getTermId() == terms.begin)
					break;
			}

			LexiconEntry le = null;
			IterablePosting p = null;


			while (!stop(lee, terms.end)) {
				le = lee.getValue();
				p = src_index.getInvertedIndex().getPostings((BitIndexPointer)lee.getValue());
				
				float max_score = 0.0f;
				while (p.next() != IterablePosting.END_OF_LIST) {
					float score = wm_model.score(1, p.getFrequency(), p.getDocumentLength(), le.getDocumentFrequency(), le.getFrequency());
					if (score > max_score)
						max_score = (float)score;
				}
				
				p.close();
				msa[le.getTermId()] = max_score;
				
				lee = lex_iter.hasNext() ? lex_iter.next() : null;
				MSGenerator.update_logger();
			}
			src_index.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return null;
	}	
	
	private static boolean stop(final Entry<String, LexiconEntry> lee, final int end)
	{
		return (lee == null || lee.getValue().getTermId() >= end);
	}

}