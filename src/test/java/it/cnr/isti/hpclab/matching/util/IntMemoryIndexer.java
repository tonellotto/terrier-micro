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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.terrier.indexing.Collection;
import org.terrier.structures.DocumentIndexEntry;
import org.terrier.structures.FieldDocumentIndexEntry;
import org.terrier.structures.Index;
import org.terrier.structures.IndexUtil;
import org.terrier.structures.SimpleDocumentIndexEntry;
import org.terrier.structures.indexing.DocumentIndexBuilder;
import org.terrier.structures.indexing.singlepass.BasicSinglePassIndexer;
import org.terrier.utility.ApplicationSetup;
import org.terrier.utility.FieldScore;

public class IntMemoryIndexer extends BasicSinglePassIndexer
{

	protected IntMemoryIndexer(long a, long b, long c) 
	{
		super(a, b, c);
	}

	public IntMemoryIndexer(String pathname, String prefix) 
	{
		super(pathname, prefix);
	}

	@Override
	public void index(Collection[] collections) 
	{
		// do nothing
	}

	public void index(final int[][] pls) throws IOException 
	{
		final String oldIndexPrefix = prefix;

		// use fresh new prefix
		prefix = oldIndexPrefix + "_1";
		fileNameNoExtension = path + ApplicationSetup.FILE_SEPARATOR + prefix;

		this.createInvertedIndex(pls);

		try {
			IndexUtil.renameIndex(path, prefix, path, oldIndexPrefix);
		} catch (IOException ioe ) {
			logger.error("Could not rename index", ioe);
		}

		// restore the prefix
		prefix = oldIndexPrefix;
		fileNameNoExtension = path + ApplicationSetup.FILE_SEPARATOR + prefix;

	}
	
	@Override
	public void createInvertedIndex()
	{
		// do nothing
	}

	
	public void createDirectIndex(Collection[] collections) 
	{
		// do nothing
	}

	@Override
	public void createInvertedIndex(Collection[] collections)
	{
		// do nothing
	}
	
	public void createInvertedIndex(final int[][] pls) throws IOException 
	{		
		numberOfDocuments = currentId = numberOfDocsSinceCheck = numberOfDocsSinceFlush = numberOfUniqueTerms = 0;
		numberOfTokens = numberOfPointers = 0;
		createMemoryPostings();
		currentIndex = Index.createNewIndex(path, prefix);
		docIndexBuilder = new DocumentIndexBuilder(currentIndex, "document");
		metaBuilder = createMetaIndexBuilder();
			
		System.gc();

		int termid = 0;
		numberOfDocuments = Integer.MIN_VALUE;
		
		for (int[] pl: pls) {
			indexPostingList(termid++, pl);
		}

		numberOfDocuments++;
		for (int i = 0; i < numberOfDocuments; ++i) {
			DocumentIndexEntry die = new SimpleDocumentIndexEntry();
			die.setDocumentLength(1);
			docIndexBuilder.addEntryToBuffer(die);
			
			Map<String, String>  docProperties = new HashMap<String, String>();
			docProperties.put("docno", String.valueOf(i));
			metaBuilder.writeDocumentEntry(docProperties);	
		}
			
		try {
			forceFlush();

			docIndexBuilder.finishedCollections();
			if (FieldScore.FIELDS_COUNT > 0) {
				currentIndex.addIndexStructure("document-factory", FieldDocumentIndexEntry.Factory.class.getName(), "java.lang.String", "${index.inverted.fields.count}");
			} else {
				currentIndex.addIndexStructure("document-factory", SimpleDocumentIndexEntry.Factory.class.getName(), "", "");
			}
			currentIndex.setIndexProperty("termpipelines", ApplicationSetup.getProperty("termpipelines", "Stopwords,PorterStemmer"));
			metaBuilder.close();
			currentIndex.flush();
						
			performMultiWayMerge();
			currentIndex.flush();
		} catch (Exception e) {
			logger.error("Problem finishing index", e);
		}
		finishedInvertedIndexBuild();
	}

	private void indexPostingList(final int termid, final int[] pl) throws IOException 
	{
		for (int docid: pl) {
			mp.add(String.valueOf(termid), docid, 1);
			numberOfDocuments = Math.max(numberOfDocuments, docid);
		}
	}
	
	/** causes the posting lists built up in memory to be flushed out */
	protected void forceFlush() throws IOException
	{	
		mp.finish(finishMemoryPosting());
		System.gc();
		createMemoryPostings();
		numberOfDocsSinceFlush = 0;	
	}

}
