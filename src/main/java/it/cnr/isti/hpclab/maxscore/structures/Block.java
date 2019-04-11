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
