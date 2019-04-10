package it.cnr.isti.hpclab.matching.structures.model;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements the DLH13 weighting model. 
 * @see <a href="http://dl.acm.org/citation.cfm?id=2037662">Upper-bound approximations for dynamic pruning</a>
 * 
 * @author Nicola Tonellotto
 */

public class DLH13 implements WeightingModel
{
	public static final double INV_LOG_2_OF_E = 1.0D / Math.log(2.0d);
	
	private double avg_l;
	private int N;

	/** {@inheritDoc} */
	@Override
	public void setup(Index index) 
	{
		checkNotNull(index);
		
		this.avg_l = index.getCollectionStatistics().getAverageDocumentLength();
		this.N = index.getCollectionStatistics().getNumberOfDocuments();
	}

	/** {@inheritDoc} */
	@Override
	public final float score(int query_freq, int term_freq, int doc_len, int doc_freq) 
	{
		throw new UnsupportedOperationException("This method should not be invoked, you need to pass 'term_freq_in_coll'!");
	}
	
	/** {@inheritDoc} */
	@Override
	public final float score(int tf_q, int x, int y, int __, int F_t)
	{
		float score = (float) Math.max(EPSILON_SCORE, (
				(tf_q/(x + 0.5d)) * INV_LOG_2_OF_E *
				(
					x * Math.log(x * (avg_l/y) * (N / ((double)F_t))) +
					0.5d * Math.log(2.0d * Math.PI * x * (1.0d - ((double)x/y)))
				)
			));
		return score;
		/*
		return (float) Math.max(EPSILON_SCORE, (
			(tf_q/(x + 0.5d)) * INV_LOG_2_OF_E *
			(
				x * Math.log(x * (avg_l/y) * (N / ((double)F_t))) +
				0.5d * Math.log(2.0d * Math.PI * x * (1.0d - ((double)x/y)))
			)
		));
		*/
	}	
}
