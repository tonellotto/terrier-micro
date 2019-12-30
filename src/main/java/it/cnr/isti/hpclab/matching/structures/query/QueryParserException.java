package it.cnr.isti.hpclab.matching.structures.query;

@SuppressWarnings("serial")
public class QueryParserException extends Exception 
{
	public QueryParserException(String message) {
		super(message);
	}
	
	public QueryParserException(Exception e) {
		super(e);
	}
}