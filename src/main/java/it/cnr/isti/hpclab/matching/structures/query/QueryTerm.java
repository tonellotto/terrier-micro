package it.cnr.isti.hpclab.matching.structures.query;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@EqualsAndHashCode
public class QueryTerm 
{			
	@Getter @Setter protected boolean required = false;
	@Getter @Setter protected float weight = 1.0f;
	@Getter @Setter protected String queryTerm;
	
	public QueryTerm(final String queryTerm)
	{
		this(queryTerm, false, 1.0f);
	}

	public QueryTerm(final String queryTerm, final boolean required)
	{
		this(queryTerm, required, 1.0f);
	}

	public QueryTerm(final String queryTerm, final float weight)
	{
		this(queryTerm, false, weight);
	}

	public QueryTerm(final String queryTerm, final boolean required, final float weight)
	{
		if (queryTerm == null || queryTerm.length() == 0)
			throw new IllegalArgumentException();

		this.queryTerm = queryTerm;
		this.weight = weight;
		this.required = required;
	}

	@Override
	public String toString() 
	{
		return (required ? "+" : "") + queryTerm + (weight == 1.0 ? "" : "^" + weight);
	}
}