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

import it.cnr.isti.hpclab.matching.structures.ResultSet;

/**
 * This class is used to return an empty list of results.
 * 
 * @author Nicola Tonellotto
 */
public class EmptyResultSet implements ResultSet
{
	private static final int EMPTY_DOCS[] = {};
	private static final float EMPTY_SCORES[] = {};
	
	/** {@inheritDoc} */
	@Override
	public int[] docids() 
	{
		return EMPTY_DOCS;
	}

	/** {@inheritDoc} */
	@Override
	public int size() 
	{
		return 0;
	}

	/** {@inheritDoc} */
	@Override
	public float[] scores() 
	{
		return EMPTY_SCORES;
	}
}
