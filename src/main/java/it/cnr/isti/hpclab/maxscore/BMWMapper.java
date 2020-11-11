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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terrier.structures.BitIndexPointer;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.ef.TermPartition;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.floats.FloatList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

//This is a function object so it must be stateless
class BMWMapper implements Function<TermPartition,TermPartition>
{
    protected static final Logger LOGGER = LoggerFactory.getLogger(BMWMapper.class);
    
    private final String src_index_path;
    private final String src_index_prefix;
    private final String wm_name;
    private final int block_size;

    public BMWMapper(final String src_index_path, final String src_index_prefix, final String wm_name, final int block_size) 
    {
        this.src_index_path = src_index_path;
        this.src_index_prefix = src_index_prefix;
        this.wm_name = wm_name;
        this.block_size = block_size;
    }

    @Override
    public TermPartition apply(TermPartition terms) 
    {        
        int written = 0;
        try {
            IndexOnDisk src_index = IndexOnDisk.createIndex(src_index_path, src_index_prefix);
            String this_prefix = src_index.getPrefix() + "_partition_" + terms.id();
            terms.prefix(this_prefix);

            if (terms.begin() >= terms.end() || terms.begin() < 0 || terms.end() > src_index.getCollectionStatistics().getNumberOfUniqueTerms()) {
                LOGGER.error("Something wrong with term positions, begin = " + terms.begin() + ", end = " + terms.end());
                return null;
            }

            WeightingModel wm_model = (WeightingModel) (Class.forName(wm_name).asSubclass(WeightingModel.class).getConstructor().newInstance());
            wm_model.setup(src_index);

            // Writers
            DataOutputStream docidsOutput  = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(src_index.getPath() + File.separator + terms.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT)));
            DataOutputStream scoresOutput  = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(src_index.getPath() + File.separator + terms.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT)));
            DataOutputStream offsetsOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(src_index.getPath() + File.separator + terms.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT)));
            long mWrittenItems = 0;

            // opening src index lexicon iterator and moving to the begin termid
            Iterator<Entry<String, LexiconEntry>> lex_iter = src_index.getLexicon().iterator();
            Entry<String, LexiconEntry> lee = null;
            for (int pos = -1; pos < terms.begin(); pos++)
                lee = lex_iter.next();

            // important, first element is block size (only in the first partition)
            if (terms.id() == 0)
                offsetsOutput.writeLong(block_size);

            LexiconEntry le = null;
            IterablePosting p = null;

            FloatList scoresList = new FloatArrayList();
            IntList   docidsList = new IntArrayList();

            while (lee != null && written < terms.end() - terms.begin()) {
                le = lee.getValue();
                p = src_index.getInvertedIndex().getPostings((BitIndexPointer)lee.getValue());
                scoresList.clear();
                docidsList.clear();

                int numBlocks = 0;
                int counter = 0;
                double score = 0.0;
                float max_score = 0.0f;
                int docid = Integer.MAX_VALUE;

                while (p.next() != IterablePosting.END_OF_LIST) {
                    docid = p.getId();
                    score = wm_model.score(1, p.getFrequency(), p.getDocumentLength(), le.getDocumentFrequency(), le.getFrequency());    
                    max_score = Math.max(max_score, (float)score);
                    if ( counter % block_size == block_size - 1) {
                        scoresList.add(max_score);
                        docidsList.add(docid); // We save the last docid of the block, not the first
                        numBlocks++;
                        max_score = 0.0f;
                    }
                    counter++;
                }
                
                // Last defective block
                if (counter % block_size != 0) {
                    scoresList.add(max_score);
                    docidsList.add(docid); // We save the last docid of the block, not the first
                    numBlocks++;
                }
                
                p.close();
                
                // Writing

                // offset
                offsetsOutput.writeLong(mWrittenItems);
                mWrittenItems += numBlocks;
                
                // Docids
                for (int d: docidsList)
                    docidsOutput.writeInt(d);
                
                // Scores
                for (float s: scoresList)
                    // Fix to slightly increase w.r.t. actual max score
                    scoresOutput.writeFloat(Math.nextAfter(s, Double.MAX_VALUE));
                
                lee = lex_iter.hasNext() ? lex_iter.next() : null;
                BMWGenerator.pb_map.step();
                written++;
            }

            src_index.close();

            offsetsOutput.close();
            scoresOutput.close();
            docidsOutput.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return terms;
    }
}