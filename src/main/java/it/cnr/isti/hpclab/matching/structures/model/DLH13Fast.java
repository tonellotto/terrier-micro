package it.cnr.isti.hpclab.matching.structures.model;

import static com.google.common.base.Preconditions.checkNotNull;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.util.FastLog;

public class DLH13Fast implements WeightingModel
{
	public static final FastLog flog = new FastLog(2, 11);
	
	private float avg_l;
	private int N;

	/** {@inheritDoc} */
	@Override
	public void setup(Index index) 
	{
		checkNotNull(index);
		
		this.avg_l = (float) index.getCollectionStatistics().getAverageDocumentLength();
		this.N = index.getCollectionStatistics().getNumberOfDocuments();
	}

	/** {@inheritDoc} */
	@Override
	public final float score(int query_freq, int term_freq, int doc_len, int doc_freq) 
	{
		throw new RuntimeException("This method should not be invoked, you need to pass 'term_freq_in_coll'!");
	}
	
	/** {@inheritDoc} */
	@Override
	public final float score(int tf_q, int x, int y, int __, int F_t)
	{
		float d_tf_q = (float)tf_q;
		float d_x    = (float)x;
		float d_y    = (float)y;
		float d_F_t  = (float)F_t;

		return Math.max(EPSILON_SCORE, (
			(d_tf_q/(d_x + 0.5f)) *
			(
				d_x * flog.log(d_x * (avg_l/d_y) * (N / (d_F_t))) +
				0.5f * flog.log(2.0f * (float)Math.PI * d_x * (1.0f - d_x/d_y))
			)
		));
	}
}
