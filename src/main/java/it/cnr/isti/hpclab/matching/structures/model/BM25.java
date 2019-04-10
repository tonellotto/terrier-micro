package it.cnr.isti.hpclab.matching.structures.model;

import org.terrier.structures.Index;
import org.terrier.utility.ApplicationSetup;

import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import lombok.Getter;
import lombok.Setter;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Random;

/**
 * This class implements the Okapi BM25 weighting model. The
 * default parameters used are:<br>
 * k_1 = 1.2<br>
 * b = 0.75<br>
 * @see <a href="http://en.wikipedia.org/wiki/Okapi_BM25">Okapi BM25 on Wikipedia</a>
 * 
 * @author Nicola Tonellotto
 */

public class BM25 implements WeightingModel
{
	@Getter @Setter private static double b = 0.75d;
	@Getter @Setter private static double k1 = 1.2d;
	
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
        double idf = Math.log(((double)num_docs - ddf + 0.5d) / (ddf + 0.5d));
        
        return Math.max(EPSILON_SCORE, idf) * (1.0d + k1);	
    }

	/** {@inheritDoc} */
	@Override
	public final float score(int term_freq_in_query, int x, int y, int df, int __)
	{
		return score(term_freq_in_query, x, y, df);
	}
	
	public static void main(String[] args)  throws IOException 
	{
		ApplicationSetup.setProperty("terrier.index.retrievalLoadingProfile.default","false");

		int num = 100_000_000;
		Random rnd = new Random(12345);
		
		int tf[] = new int[num];
		int dl[] = new int[num];
		int df[] = new int[num];
		int TF[] = new int[num];
		
		for (int i = 0; i < num; i++) {
			tf[i] = rnd.nextInt(1_000);
			dl[i] = Math.max(tf[i], rnd.nextInt(10_000));
			df[i] = rnd.nextInt(100_000);
			TF[i] = df[i] * tf[i];
		}
		WeightingModel wm = new BM25();
		wm.setup(Index.createIndex("/Users/khast/index-java","cw12b.ef"));
		
		long start = System.currentTimeMillis();
		for (int i = 0; i < num; i++) {
			wm.score(1, tf[i], dl[i], df[i], TF[i]);
		}
		
		System.err.println((System.currentTimeMillis() - start) / 1000.0d);
	}
}
