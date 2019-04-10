package it.cnr.isti.hpclab.matching.structures.resultset;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import it.cnr.isti.hpclab.matching.structures.ResultSet;

/**
 * This class is used to return a docid-only list of results.
 *  
 * @author Nicola Tonellotto
 */
public class DocidResultSet implements ResultSet
{
	private int docids[];
	
	/**
	 * Constructs a result set using the docids provided.
	 * 
	 * @param docids the list of docids to use (must be not null)
	 */
	public DocidResultSet(List<Integer> docids)
	{
		checkNotNull(docids);
		this.docids = new int[docids.size()];
		for (int i = 0; i < docids.size(); i++)
			this.docids[i] = docids.get(i);
	}

	/** {@inheritDoc} */
	@Override
	public int[] docids() 
	{
		return docids;
	}

	/** {@inheritDoc} */
	@Override
	public int size() 
	{
		return docids.length;
	}

	/**
	 * Throw an unsupported operation exception if invoked, i.e., must not be invoked.
	 * 
	 * @throws unsupported operation exception if invoked
	 */
	@Override
	public float[] scores() 
	{
		throw new UnsupportedOperationException();
	}
}
