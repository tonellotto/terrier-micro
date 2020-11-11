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
import lombok.Getter;
import lombok.Setter;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The only difference w.r.t. BM25 class is that IDF function uses log1p instead of log.
 * This class implements the "accurate" version, i.e., document lengths are not normalized
 * to 8 bits and pre-computed. 
 * 
 * @author Nicola Tonellotto
 */

public class LuceneBM25 implements WeightingModel
{
	@Getter @Setter private static double b = Double.parseDouble(System.getProperty("bm25.b", "0.75"));
	@Getter @Setter private static double k1 = Double.parseDouble(System.getProperty("bm25.k1", "1.2"));
	
	private double avg_doc_len;
	private int num_docs;
	
	/** {@inheritDoc} */
	@Override
	public void setup(Index index) 
	{
		checkNotNull(index);
		
		this.avg_doc_len = index.getCollectionStatistics().getAverageDocumentLength();
		this.num_docs = index.getCollectionStatistics().getNumberOfDocuments();
	}

	/** {@inheritDoc} */
	@Override
	public final float score(int query_freq, int x, int y, int df) 
	{
		return (float) (query_freq * TF(x, y) * IDF(df));
	}

	/**
	 * Compute the document-dependent component of BM25, aka TF, with document-length normalization.
	 * 
	 * @param qdf the term frequency in the document, i.e., the number of occurrences of term in document
	 * @param doc_len the length of the document to score, in tokens
	 * @return the document-dependent component of BM25
	 */
	private final double TF(int term_freq, int doc_len)
	{
		double dtf = (double)term_freq;
        return dtf / (dtf + k1 * (1.0d - b + b * (double)doc_len/avg_doc_len));
	}

	/**
	 * Compute the document-independent component of BM25, aka IDF.
	 * 
	 * @param df the term document frequency, i.e., the number of documents in which the term appears at least once
	 * @return the document-independent component of BM25
	 */
	private final double IDF(int doc_freq) 
	{
		double ddf = (double)doc_freq;
        double idf = Math.log1p(((double)num_docs - ddf + 0.5d) / (ddf + 0.5d));
        
        return Math.max(EPSILON_SCORE, idf) * (1.0d + k1);	
    }

	/** {@inheritDoc} */
	@Override
	public final float score(int term_freq_in_query, int x, int y, int df, int __)
	{
		return score(term_freq_in_query, x, y, df);
	}	
}
