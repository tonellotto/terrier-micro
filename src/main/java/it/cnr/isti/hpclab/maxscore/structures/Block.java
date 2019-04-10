package it.cnr.isti.hpclab.maxscore.structures;

import org.terrier.structures.postings.IterablePosting;

public class Block 
{
	public static int END_OF_BLOCK = IterablePosting.END_OF_LIST;
	
	public long mDocid;
	public float mMaxScore;
	
	public Block(long mDocid, float mMaxScore)
	{
		this.mDocid = mDocid;
		this.mMaxScore = mMaxScore;
	}
	
	public long getDocid() 
	{
		return mDocid;
	}
	
	public void setDocid(long mDocid) 
	{
		this.mDocid = mDocid;
	}
	
	public float getMaxScore() 
	{
		return mMaxScore;
	}
	
	public void setMaxScore(float mMaxScore) 
	{
		this.mMaxScore = mMaxScore;
	}
	
	@Override
	public String toString()
	{
		return "(" + mDocid + "," + mMaxScore + ")";
	}
}
