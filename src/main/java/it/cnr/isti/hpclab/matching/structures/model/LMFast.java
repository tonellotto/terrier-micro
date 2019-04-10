package it.cnr.isti.hpclab.matching.structures.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.util.FastLog;

public class LMFast implements WeightingModel
{
	public static final FastLog flog = new FastLog(Math.E, 11);
	public static float mu = 2500.0f;
	
	private long num_tokens_in_coll;

	/** {@inheritDoc} */
	@Override
	public void setup(Index index) 
	{
		checkNotNull(index);
		
		this.num_tokens_in_coll = index.getCollectionStatistics().getNumberOfTokens();
	}

	/** {@inheritDoc} */
	@Override
	public final float score(int query_freq, int term_freq, int doc_len, int doc_freq) 
	{
		throw new UnsupportedOperationException("This method should not be invoked, you need to pass 'term_freq_in_coll'!");
	}
	
	/** {@inheritDoc} */
	@Override
	public final float score(int __, int x, int y, int ___, int F_t)
	{
		float d_x = (float)x;
		float d_y = (float)y;
		float d_F_t = (float)F_t;
		// return (float) Math.max(EPSILON_SCORE, (Math.log(1 + (d_x/(mu * (d_F_t/num_tokens_in_coll)))) + Math.log(mu/(mu+d_y))));
		return Math.max(EPSILON_SCORE, (flog.log(1 + (d_x/(mu * (d_F_t/num_tokens_in_coll)))) + flog.log(mu/(mu+d_y))));
	}
}
