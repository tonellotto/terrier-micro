package it.cnr.isti.hpclab.maxscore.structures;

import java.io.IOException;
import java.util.NoSuchElementException;

public class BlockEnumerator 
{	
	private final int[] mDocids;
	private final float[] mScores;
	
	private final int mBegin;
	private int mPos;
	private final int mNumBlocks;
		
	public BlockEnumerator(long offset, int numBlocks, final int[] docids, final float[] scores) throws IOException
	{
		this.mDocids = docids;
		this.mScores = scores;
		
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
    		return mScores[mBegin + mPos];
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
		return "[" + mDocids[mBegin + mPos] + "," + mScores[mBegin + mPos] + "]";
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
