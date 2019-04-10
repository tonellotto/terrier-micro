package it.cnr.isti.hpclab.matching.structures.resultset;

import it.cnr.isti.hpclab.matching.structures.ResultSet;

/**
 * This class is used to return an empty list of results.
 * 
 * @author Nicola Tonellotto
 */
public class EmptyResultSet implements ResultSet
{
	private static final int EMPTY_DOCS[] = {};
	private static final float EMPTY_SCORES[] = {};
	
	/** {@inheritDoc} */
	@Override
	public int[] docids() 
	{
		return EMPTY_DOCS;
	}

	/** {@inheritDoc} */
	@Override
	public int size() 
	{
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public float[] scores() 
	{
		return EMPTY_SCORES;
	}
}
