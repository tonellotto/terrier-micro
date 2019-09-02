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
package it.cnr.isti.hpclab.matching.structures.model;

import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This class implements the Bayesian smoothing with Dirichlet Prior weighting model. The
 * default parameters used are:<br>
 * \mu = 2500<br>
 * @see <a href="http://dl.acm.org/citation.cfm?id=2037662">Upper-bound approximations for dynamic pruning</a>
 * 
 * @author Nicola Tonellotto
 */

public class LM implements WeightingModel
{
	public static double mu = 2500.0d;
	
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
	public final float score(int term_freq_in_query, int x, int y, int doc_freq_in_coll, int F_t)
	{
		double d_x = (double)x;
		double d_y = (double)y;
		double d_F_t = (double)F_t;
		// return (float) Math.max(EPSILON_SCORE, term_freq_in_query * (Math.log(1 + (d_x/(mu * (d_F_t/num_tokens_in_coll)))) + Math.log(mu/(mu+d_y))));
		return (float) Math.max(EPSILON_SCORE, term_freq_in_query * Math.log( (d_x + mu * d_F_t / num_tokens_in_coll)) / (d_y + mu));
	}
}
