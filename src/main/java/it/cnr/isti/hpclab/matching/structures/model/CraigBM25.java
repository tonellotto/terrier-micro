package it.cnr.isti.hpclab.matching.structures.model;

import org.terrier.matching.models.WeightingModelLibrary;
import org.terrier.structures.Index;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import lombok.Getter;
import lombok.Setter;

import static com.google.common.base.Preconditions.checkNotNull;

public class CraigBM25 implements WeightingModel
{
	@Getter @Setter private double b  = Double.parseDouble(System.getProperty("bm25.b", "0.75"));
	@Getter @Setter private double k1 = Double.parseDouble(System.getProperty("bm25.k1", "1.2"));
	@Getter @Setter private double k3 = Double.parseDouble(System.getProperty("bm25.k3", "8.0"));
	
	private double avg_doc_len;
	private int num_docs;

	public CraigBM25()
	{
	}
	
	public CraigBM25(final double k1, final double b)
	{
		this.b = b;
		this.k1 = k1;
	}
	
	@Override
	public String toString()
	{
		return "Terrier BM25" + " k1 = " + k1 + " b = " + b;
	}


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
		final double K = k1 * ((1d - b) + b * y / avg_doc_len);
		return (float) (WeightingModelLibrary.log((num_docs - df + 0.5d) / (df + 0.5d)) *
				((k1 + 1d) * x / (K + x)) *
				((k3 + 1d) * 1 / (k3 + 1d)));
	}

	/** {@inheritDoc} */
	@Override
	public final float score(int term_freq_in_query, int x, int y, int df, int __)
	{
		return score(term_freq_in_query, x, y, df);
	}
}