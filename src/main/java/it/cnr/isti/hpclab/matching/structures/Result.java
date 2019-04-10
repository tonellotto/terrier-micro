package it.cnr.isti.hpclab.matching.structures;

/**
 *  This class represent a the top-k candidate documents set,
 *  composed by a docid and an associated score. Results are ordered
 *  in decreasing score, i.e. greater first.
 *  
 *  @author Nicola Tonellotto
 */
@SuppressWarnings("serial")
public class Result implements Comparable<Result>, java.io.Serializable
{
	private int docid;
	private float score;

	/**
	 * Create a new result.
	 * 
	 * @param id the docid
	 * @param s the (initial) score
	 */
	public Result(int id, float s)
	{
		docid = id;
		score = s;
	}

	/**
	 * Create a new results, with 0 (initial) score
	 * 
	 * @param id the docid
	 */
	public Result(int id)
	{
		this(id, 0);
	}

	/**
	 * Compares this object with the specified object for score-decreasing ordering.
	 * 
	 * @param o the object to be compared
	 * @return a negative integer, zero, or a positive integer as this object is greater than, equal to, or less than the specified object.
	 */
	public int compareTo(final Result o) 
	{
		if (this.score < o.score)
			return -1;
		else if (this.score > o.score)
			return 1;
		else
			return 0;
	}
	
	/**
	 * Return the result's docid.
	 * @return the result's docid
	 */
	public int getDocId() 
	{ 
		return docid; 
	}
	
	/**
	 * Return the result's score.
	 * @return the result's score
	 */
	public float getScore() 
	{ 
		return score; 
	}
	
	/**
	 * Update the result's score.
	 * @param update the score to be added to the result's current score
	 */
	public void updateScore(float update) 
	{ 
		this.score += update; 
	}
	
	@Override
	public String toString()
	{
		return "[" + docid + "](" + score + ")";
	}

	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + docid;
		result = prime * result + Float.floatToIntBits(score);
		return result;
	}

	@Override
	public boolean equals(Object obj) 
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Result other = (Result) obj;
		if (docid != other.docid)
			return false;
		if (Float.floatToIntBits(score) != Float.floatToIntBits(other.score))
			return false;
		return true;
	}
	
	/*
	public void setDocid(int id) 
	{ 
		this.docid = id; 
	}

	public void resetScore() 
	{ 
		this.score = 0.0; 
	}
	*/
	
	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException 
	{
		s.defaultWriteObject();
		s.writeInt(docid);
		s.writeFloat(score);
	}
	
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException 
	{
		s.defaultReadObject();
		docid = s.readInt();
		score = s.readFloat();
	}

}
