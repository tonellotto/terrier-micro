package it.cnr.isti.hpclab.matching.structures;

/**
 * This interface must be implemented by the classes used for returning the results of query processing.
 * 
 * @author Nicola Tonellotto
 */
public interface ResultSet
{			
	/**
	 * Return the documents ids after retrieval.
	 * 
	 * @return the documents ids after retrieval
	 */
	int[] docids();
		
	/**
	 * Return the effective size of the result set.
	 * 
	 * @return the size of the result set
	 */
	int size();
	
	/**
	 * Return the documents scores after retrieval.
	 * 
	 * @return the documents scores after retrieval
	 */
	float[] scores();	
}
