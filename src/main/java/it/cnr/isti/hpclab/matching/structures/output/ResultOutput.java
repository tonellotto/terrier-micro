package it.cnr.isti.hpclab.matching.structures.output;

import java.io.Closeable;
import java.io.IOException;

import org.apache.log4j.Logger;

import it.cnr.isti.hpclab.matching.structures.SearchRequest;

public abstract class ResultOutput implements Closeable
{
	protected static final Logger LOGGER = Logger.getLogger(ResultOutput.class);
	
	public static ResultOutput newInstance(String type)
	{
		if (type == null || "null".equals(type)) {
			return new NullResultOutput();
		} else if ("docid".equalsIgnoreCase(type)) {
			return new DocidResultOutput();
		} else if ("score".equalsIgnoreCase(type)) {
			return new ScoredResultOutput();
		} else if ("docno".equalsIgnoreCase(type)) {
			return new DocnoResultOutput();
		} else if ("trec".equalsIgnoreCase(type)) {
			return new TrecResultOutput();
		} 
 

		throw new IllegalArgumentException("Unknown ResultOutput type");
	}
	
	/** 
	 * Outputs the results of search request rq
	 * 
	 * @param re the search request
	 */
	public abstract void print(final SearchRequest rq) throws IOException;
}
