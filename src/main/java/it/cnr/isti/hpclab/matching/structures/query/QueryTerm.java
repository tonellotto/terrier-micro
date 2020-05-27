package it.cnr.isti.hpclab.matching.structures.query;

public class QueryTerm 
{			
	protected boolean required = false;
	protected float weight = 1.0f;
	protected String queryTerm;
	
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
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((queryTerm == null) ? 0 : queryTerm.hashCode());
		result = prime * result + (required ? 1231 : 1237);
		result = prime * result + Float.floatToIntBits(weight);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryTerm other = (QueryTerm) obj;
		if (queryTerm == null) {
			if (other.queryTerm != null)
				return false;
		} else if (!queryTerm.equals(other.queryTerm))
			return false;
		if (required != other.required)
			return false;
		if (Float.floatToIntBits(weight) != Float.floatToIntBits(other.weight))
			return false;
		return true;
	}

	public boolean isRequired() 
	{
		return required;
	}

	public void setRequired(boolean required) 
	{
		this.required = required;
	}

	public float getWeight() 
	{
		return weight;
	}

	public void setWeight(float weight) 
	{
		this.weight = weight;
	}

	public String getQueryTerm() 
	{
		return queryTerm;
	}

	public void setQueryTerm(String queryTerm) 
	{
		this.queryTerm = queryTerm;
	}
}