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

package it.cnr.isti.hpclab.matching.structures.resultset;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import it.cnr.isti.hpclab.matching.structures.ResultSet;

/**
 * This class is used to return a docid-only list of results.
 *  
 * @author Nicola Tonellotto
 */
public class DocidResultSet implements ResultSet
{
	private int docids[];
	
	/**
	 * Constructs a result set using the docids provided.
	 * 
	 * @param docids the list of docids to use (must be not null)
	 */
	public DocidResultSet(List<Integer> docids)
	{
		checkNotNull(docids);
		this.docids = new int[docids.size()];
		for (int i = 0; i < docids.size(); i++)
			this.docids[i] = docids.get(i);
	}

	/** {@inheritDoc} */
	@Override
	public int[] docids() 
	{
		return docids;
	}

	/** {@inheritDoc} */
	@Override
	public int size() 
	{
		return docids.length;
	}

	/**
	 * Throw an unsupported operation exception if invoked, i.e., must not be invoked.
	 * 
	 * @throws unsupported operation exception if invoked
	 */
	@Override
	public float[] scores() 
	{
		throw new UnsupportedOperationException();
	}
}
