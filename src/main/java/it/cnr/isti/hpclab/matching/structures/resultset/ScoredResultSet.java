package it.cnr.isti.hpclab.matching.structures.resultset;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import it.cnr.isti.hpclab.matching.structures.Result;
import it.cnr.isti.hpclab.matching.structures.ResultSet;
import it.cnr.isti.hpclab.matching.structures.TopQueue;
import it.unimi.dsi.fastutil.PriorityQueue;

import java.io.IOException;
import java.util.List;

/**
 * This class is used to return a docid-with-score list of results.
 *  
 * @author Nicola Tonellotto
 */

public class ScoredResultSet implements ResultSet 
{
	private int mDocids[];
	private float mScores[];
	
	/**
	 * Constructs a result set using the docids and scores provided.
	 * 
	 * @param docids the list of docids to use (must be not null)
	 * @param scores the list of scores to use (must be not null)
	 */
	public ScoredResultSet(final List<Integer> docids, final List<Float> scores)
	{
		checkNotNull(docids);
		checkNotNull(scores);
		checkArgument(docids.size() == scores.size());
		this.mDocids = new int[docids.size()];
		this.mScores = new float[scores.size()];
		for (int i = 0; i < docids.size(); i++) {
			this.mDocids[i] = docids.get(i);
			this.mScores[i] = scores.get(i);
		}
	}

	public ScoredResultSet(final int[] docids, final float[] scores)
	{
		checkNotNull(docids);
		checkNotNull(scores);
		checkArgument(docids.length == scores.length);
		this.mDocids = docids;
		this.mScores = scores;
	}

	/**
	 * Constructs a result set using the docids and scores stored in the provided queue.<br>
	 * The queue ordering in decreasing score in preserved.
	 * 
	 * @param queue the queue storing the results in decreasing order of scores (must be not null)
	 */
	public ScoredResultSet(final TopQueue queue)
	{
		try {
			checkNotNull(queue);
		
			PriorityQueue<Result> q = queue.copy().top();
			int N = q.size();
			this.mDocids = new int[N];
			this.mScores = new float[N];
		
			int i = 0;
			while (!q.isEmpty()) {
				Result res = q.dequeue();
				this.mDocids[N - 1 - i] = res.getDocId();
				this.mScores[N - 1 - i] = res.getScore();
				i++;
			}

		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
	}

	/** {@inheritDoc} */
	@Override
	public int[] docids() {
		return mDocids;
	}

	/** {@inheritDoc} */
	@Override
	public int size() {
		return mDocids.length;
	}

	/** {@inheritDoc} */
	@Override
	public float[] scores() {
		return mScores;
	}
}
