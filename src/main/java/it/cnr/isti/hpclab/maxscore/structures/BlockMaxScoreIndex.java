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

package it.cnr.isti.hpclab.maxscore.structures;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;

import org.apache.log4j.Logger;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

public class BlockMaxScoreIndex
{
	protected static final Logger logger = Logger.getLogger(BlockMaxScoreIndex.class);
	
	public static final String DOCID_EXT  = ".docids";
	public static final String SCORE_EXT  = ".scores";
	public static final String OFFSET_EXT = ".offsets";
		
	public static final String STRUCTURE_NAME = ".bmw";
	
	/** The number of entries in the offsets file minus 2, equal to the number of terms in lexicon.*/
	private int numEntries;
	/** The size of a block, in terms of postings. */
	private int blockSize;
	/** The total number of blocks stored, i.e., the total number of docids and scores stored, i.e., the size of mDocids and mScores arrays. */
	private long totalBlocks;
		
	private long[]  mOffsetsArray;
	private int[]   mDocidsArray;
	private float[] mScoresArray;
	
	public BlockMaxScoreIndex(final Index index) throws IOException
	{
		createBlockMaxScoreIndex(((IndexOnDisk)index).getPath() + File.separator + ((IndexOnDisk)index).getPrefix() + STRUCTURE_NAME);
	}
	
	@SuppressWarnings("resource")
	private void createBlockMaxScoreIndex(String filenamePrefix) throws IOException
	{
		FileChannel offsetsFC = new FileInputStream(new File(filenamePrefix + OFFSET_EXT)).getChannel();
		LongBuffer offsetsBuffer = offsetsFC.map(FileChannel.MapMode.READ_ONLY, 0, offsetsFC.size()).asLongBuffer();
		mOffsetsArray = new long[offsetsBuffer.remaining()];
		offsetsBuffer.get(mOffsetsArray);
		offsetsFC.close();

		FileChannel docidsFC = new FileInputStream(new File(filenamePrefix + DOCID_EXT)).getChannel();
		IntBuffer docidsBuffer = docidsFC.map(FileChannel.MapMode.READ_ONLY, 0, docidsFC.size()).asIntBuffer();
		mDocidsArray = new int[docidsBuffer.remaining()];
		docidsBuffer.get(mDocidsArray);
		docidsFC.close();

		FileChannel scoresFC = new FileInputStream(new File(filenamePrefix + SCORE_EXT)).getChannel();
		FloatBuffer scoresBuffer = scoresFC.map(FileChannel.MapMode.READ_ONLY, 0, scoresFC.size()).asFloatBuffer();
		mScoresArray = new float[scoresBuffer.remaining()];
		scoresBuffer.get(mScoresArray);
		scoresFC.close();

		// numEntries = (int) (mOffsetsFC.size() / (long)LONG_BYTES) - 2;
		this.numEntries = mOffsetsArray.length - 2;
		// blockSize = (int) mOffsetsBuffer.get();
		this.blockSize = (int) mOffsetsArray[0];
		// totalBlocks = mOffsetsBuffer.get(numEntries + 1);
		this.totalBlocks = mOffsetsArray[numEntries + 1];
	}

	public BlockEnumerator get(int i) throws IOException
	{
		int num_blocks = (int) (( i == numEntries - 1 )
			  	   ? totalBlocks - mOffsetsArray[i + 1]
		           : mOffsetsArray[i + 2] - mOffsetsArray[i + 1]);

		return new BlockEnumerator(mOffsetsArray[i + 1], num_blocks, mDocidsArray, mScoresArray);
	}
	
	public int getBlockSize()
	{
		return this.blockSize;
	}
}
