package it.cnr.isti.hpclab.matching.structures;

import org.terrier.structures.Index;
import org.terrier.structures.LexiconEntry;
import org.terrier.structures.postings.Posting;

/**
 * This interface must be implemented by the classes used for weighting terms and documents.
 * 
 * @author Nicola Tonellotto
 */
public interface WeightingModel 
{
	static final float EPSILON_SCORE = 1.0E-6f;
	/** 
	 * Set up the index used to lookup statistics
	 *  
	 * @param index the index to use
	 */
	void setup(final Index index);

	/** 
	 * Compute the score document w.r.t. a single term that may apper multiple times in a query, i.e.,
	 * a query composed by a single term, with multiple occurrences.
	 *  
	 * @param term_freq_in_query the term frequency (in query)
	 * @param term_freq_in_doc the term frequency (in document)
	 * @param doc_len the length of the document to score, in tokens
	 * @param doc_freq_in_coll the document frequency (in collection)
	 * @param term_freq_in_coll the term frequency (in collection)
	 * 
	 * @return the score of the document for the given term
	 */
	float score(int query_freq, int term_freq, int doc_len, int doc_freq);
	
	float score(int term_freq_in_query, int term_freq_in_doc, int doc_len, int doc_freq_in_coll, int term_freq_in_coll);
	
	default float score(int term_freq_in_query, final Posting p, final LexiconEntry le) 
	{
		return score(term_freq_in_query, p.getFrequency(), p.getDocumentLength(), le.getDocumentFrequency(), le.getFrequency());
	}
}
