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

import java.io.IOException;
import java.util.NoSuchElementException;

public class BlockEnumerator 
{	
	private final int[] mDocids;
	private final float[] mScores;
	private final float weight;
	
	private final int mBegin;
	private int mPos;
	private final int mNumBlocks;
		
	public BlockEnumerator(long offset, int numBlocks, final int[] docids, final float[] scores, final float weight) throws IOException
	{
		this.mDocids = docids;
		this.mScores = scores;
		this.weight  = weight;
		
		this.mBegin = (int) offset;
		// this.mPos = 0;
		this.mPos = -1;
		this.mNumBlocks = numBlocks;		
	}
	
	public boolean hasNext()
	{
		return (mPos + 1 < mNumBlocks);
	}
	
	public void next() 
	{
		if (mPos < mNumBlocks) {
			mPos++;
		} else {
			throw new NoSuchElementException("the block enumerator has no more elements");
		}
	}
	
    public void move(long docid)
    {
    	if (mPos == -1)
    		mPos++;
    	while (last() < docid) {
    		if (mPos == mNumBlocks) { // We move beyond the last block, what we are looking for (docid) can't be found.
    			return;
    		}
    		mPos++;
    	}    		
    }
    
    public int last() 
    {
    	if (mPos < mNumBlocks)
    		return mDocids[mBegin + mPos];
    	return Integer.MAX_VALUE;
    }

    public float score()
    {
    	if (mPos < mNumBlocks)
    		return weight * mScores[mBegin + mPos];
    	return 0.0f;
    	// throw new NoSuchElementException("Can't get score when END_OF_BLOCK has been reached");
    }
	
    /**
     * Return the number of blocks.
     * 
     * @return the number of blocks
     */
    public int size() 
    {
        return mNumBlocks;
    }

	/** {@inheritDoc} */
	@Override
	public String toString()
	{
		if (this.mNumBlocks == 0)
			return "[]";
		return "[" + mDocids[mBegin + mPos] + "," + weight * mScores[mBegin + mPos] + "]";
		/*
		StringBuffer buf = new StringBuffer();
		buf.append("[");
		if (mPos == 0)
			buf.append("<" + mDocids[mBegin] + "-" + mScores[mBegin] + ">");
		else
			buf.append("" + mDocids[mBegin] + "-" + mScores[mBegin] + "");
		
		for (int i = 1; i < mNumBlocks; ++i)
			if (i == mPos)
				buf.append(", <" + mDocids[mBegin + i] + "-" + mScores[mBegin + i] + ">");
			else
				buf.append(", " + mDocids[mBegin + i] + "-" + mScores[mBegin + i] + "");
		buf.append("]");
		return buf.toString();
		*/
	}
	
	public void reset()
	{
		this.mPos = -1;
	}
}