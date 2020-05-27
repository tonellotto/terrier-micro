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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terrier.structures.BitIndexPointer;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.IterablePosting;

import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;
import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;
import it.cnr.isti.hpclab.matching.MatchingSetupTest;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.matching.structures.model.BM25;

public class IndexCreationTest extends MatchingSetupTest
{
    protected static IndexOnDisk originalIndex = null;
    protected static IndexOnDisk succinctIndex = null;
    
    @BeforeClass public static void createIndexesOnDisk() throws Exception
    {
        MatchingSetupTest.makeEnvironment();
        MatchingSetupTest.doWT10GSampleIndexing();
        
        String args[] = {"-path", terrierEtc, 
                         "-prefix", "data" + EliasFano.USUAL_EXTENSION, 
                         "-index", terrierEtc + File.separator + "data.properties", 
                         "-p", Integer.toString(3)};
        System.setProperty(EliasFano.LOG2QUANTUM, "3");
        Generator.main(args);
                
        loadIndexes();
        createMaxScoreIndexes();
    }
    
    private static void createMaxScoreIndexes() throws IOException
    {
        if (Files.exists(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + MaxScoreIndex.USUAL_EXTENSION)))
            Files.delete(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + MaxScoreIndex.USUAL_EXTENSION));
        
        String argsMS1[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".properties",
                            "-wm", "BM25",
                            "-p", Integer.toString(2)};

        String argsMS2[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef.properties",
                            "-wm", "BM25",
                            "-p", Integer.toString(2)};

        MSGenerator.main(argsMS1);
        MSGenerator.main(argsMS2);
    }
    
    private static void loadIndexes()
    {
        originalIndex = IndexOnDisk.createIndex();
        succinctIndex = IndexOnDisk.createIndex(originalIndex.getPath(), originalIndex.getPrefix() + EliasFano.USUAL_EXTENSION);
    }
    
    @AfterClass public static void clean() 
    {
        deleteTerrierEtc();
    }
        
    @Before public void openIndex() throws IOException
    {
        loadIndexes();
    }
    
    @After public void closeIndex() throws IOException
    {
        originalIndex.close();
        succinctIndex.close();
    }

    @Test
    public void testOriginal() throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException
    {
        IndexOnDisk index = originalIndex;
        MaxScoreIndex msi = new MaxScoreIndex(index);
        
        WeightingModel wm_model = (WeightingModel) (Class.forName(BM25.class.getName()).asSubclass(WeightingModel.class).getConstructor().newInstance());
        wm_model.setup(index);
        
        for (int i = 0; i < index.getCollectionStatistics().getNumberOfUniqueTerms(); i++) {
            Map.Entry<String, LexiconEntry> mEntry = index.getLexicon().getIthLexiconEntry(i);
            String term = mEntry.getKey();

            IterablePosting posting = index.getInvertedIndex().getPostings((BitIndexPointer) index.getLexicon().getLexiconEntry(term));
            float max_score = 0.0f;
            while (posting.next() != IterablePosting.END_OF_LIST) {
                double score = wm_model.score(1, posting.getFrequency(), posting.getDocumentLength(), mEntry.getValue().getDocumentFrequency());
                if (score > max_score)
                    max_score = (float)score;
            }
            assertEquals("Problems with termid " + mEntry.getValue().getTermId(), max_score, msi.getMaxScore(i), 1e-6);
        }
        
        msi.close();
    }

    @Test
    public void testSuccinct() throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException
    {
        IndexOnDisk index = succinctIndex;
        MaxScoreIndex msi = new MaxScoreIndex(index);
        
        WeightingModel wm_model = (WeightingModel) (Class.forName(BM25.class.getName()).asSubclass(WeightingModel.class).getConstructor().newInstance());
        wm_model.setup(index);
        
        for (int i = 0; i < index.getCollectionStatistics().getNumberOfUniqueTerms(); i++) {
            Map.Entry<String, LexiconEntry> mEntry = index.getLexicon().getIthLexiconEntry(i);
            String term = mEntry.getKey();

            IterablePosting posting = index.getInvertedIndex().getPostings((BitIndexPointer) index.getLexicon().getLexiconEntry(term));
            float max_score = 0.0f;

            while (posting.next() != IterablePosting.END_OF_LIST) {
                double score = wm_model.score(1, posting.getFrequency(), posting.getDocumentLength(), mEntry.getValue().getDocumentFrequency());
                if (score > max_score)
                    max_score = (float)score;
            }
            assertEquals("Problems with termid " + mEntry.getValue().getTermId(), max_score, msi.getMaxScore(i), 1e-6);
        }
        
        msi.close();
    }

}
