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
package it.cnr.isti.hpclab.matching.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.terrier.compression.bit.BitIn;
import org.terrier.indexing.Collection;
import org.terrier.indexing.Document;
import org.terrier.indexing.FileDocument;
import org.terrier.indexing.tokenisation.EnglishTokeniser;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.structures.indexing.Indexer;
import org.terrier.structures.indexing.classical.BlockIndexer;
import org.terrier.structures.indexing.singlepass.BasicSinglePassIndexer;
import org.terrier.utility.ApplicationSetup;

import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;

public class IndexUtils 
{
	private static int count = 0;

	public static Index makeTerrierIndex(final String[] docnos, final String[] documents) 
	{
		return makeTerrierIndex(docnos, documents, false);
	}

	public static Index makeTerrierPositionalIndex(final String[] docnos, final String[] documents) 
	{
		return makeTerrierIndex(docnos, documents, true);
	}

	public static Index makeTerrierIndex(final String[] docnos, final String[] documents, boolean with_blocks) 
	{
		count++;
		Indexer indexer = !with_blocks 
				? new BasicSinglePassIndexer(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX + '-' + count)
				: new BlockIndexer(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX + '-' + count);
	
		return makeIndex(docnos, documents, indexer,
						 ApplicationSetup.TERRIER_INDEX_PATH, 
						 ApplicationSetup.TERRIER_INDEX_PREFIX + '-' + count);
	}

	private static Index makeIndex(final String[] docnos, final String[] documents, final Indexer indexer, final String path, final String prefix) 
	{
		assert docnos.length == documents.length;
		
		if (IndexOnDisk.existsIndex(path, prefix)) 
			throw new IllegalStateException("Index at " + path + "," + prefix + " already exists!");
		
		Collection col = makeCollection(docnos, documents);
		indexer.index(new Collection[] { col });
		
		return Index.createIndex(path, prefix);
	}

	private static Collection makeCollection(final String[] docnos, final String[] documents) 
	{
		assert docnos.length == documents.length;
		
		Document[] sourceDocs = new Document[docnos.length];
		Map<String, String> docProperties;
		
		for (int i = 0; i < docnos.length; i++) {
			docProperties = new HashMap<String, String>();
			docProperties.put("docno", docnos[i]);
			sourceDocs[i] = new FileDocument(new ByteArrayInputStream(documents[i].getBytes()), docProperties, new EnglishTokeniser());
		}
		
		Collection col = new CollectionDocumentList(sourceDocs);
		return col;
	}

	public static Index makeTerrierIndex(final int[][] postingLists) throws IOException 
	{
		count++;
		IntMemoryIndexer indexer = new IntMemoryIndexer(ApplicationSetup.TERRIER_INDEX_PATH, ApplicationSetup.TERRIER_INDEX_PREFIX + '-' + count);
		return makeIndex(postingLists, indexer,
						 ApplicationSetup.TERRIER_INDEX_PATH, 
						 ApplicationSetup.TERRIER_INDEX_PREFIX + '-' + count);
	}

	private static Index makeIndex(final int[][] postingLists, final IntMemoryIndexer indexer, final String path, final String prefix) throws IOException 
	{
		if (IndexOnDisk.existsIndex(path, prefix)) 
			throw new IllegalStateException("Index at " + path + "," + prefix + " already exists!");

		indexer.index(postingLists);
		return Index.createIndex(path, prefix);
	}
	
	public static Index makeEFIndex(final Index original)
	{
		return makeEFIndex(original, false);
	}
	
	public static Index makeEFIndex(final Index original, boolean with_pos)
	{
		String args[];
		if (!with_pos)
			args = new String[8];
		else 
			args = new String[9];
		
		args[0] = "-path"; 
		args[1] = ((IndexOnDisk)original).getPath(); 
		args[2] = "-prefix";
		args[3] = ((IndexOnDisk)original).getPrefix() + EliasFano.USUAL_EXTENSION;
		args[4] = "-index";
		args[5] = ((IndexOnDisk)original).getPath() + File.separator + ((IndexOnDisk)original).getPrefix() + ".properties";
		args[6] = "-p";
		args[7] = "2";
		
		if (with_pos)
			args[8] = "-b";
		
		System.setProperty(EliasFano.LOG2QUANTUM, "3");
		Generator.main(args);
		
		return Index.createIndex(((IndexOnDisk)original).getPath(), ((IndexOnDisk)original).getPrefix() + EliasFano.USUAL_EXTENSION);
	}
	
	public static long getInvertedIndexSize(final Index index) throws IOException
	{
		if (index.getInvertedIndex().getClass() == org.terrier.structures.bit.BitPostingIndex.class) {
			Path filePath = Paths.get(((IndexOnDisk)index).getPath() + File.separator + ((IndexOnDisk)index).getPrefix() + ".inverted" + BitIn.USUAL_EXTENSION);
		    return FileChannel.open(filePath).size();
		} else if (index.getInvertedIndex().getClass() == it.cnr.isti.hpclab.ef.structures.EFInvertedIndex.class) {
			Path filePath1 = Paths.get(((IndexOnDisk)index).getPath() + File.separator + ((IndexOnDisk)index).getPrefix() + EliasFano.DOCID_EXTENSION);
		    Path filePath2 = Paths.get(((IndexOnDisk)index).getPath() + File.separator + ((IndexOnDisk)index).getPrefix() + EliasFano.FREQ_EXTENSION);
		    
		    return FileChannel.open(filePath1).size() + FileChannel.open(filePath2).size();
		}
		return 0l;
	}
}
