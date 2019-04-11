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

package it.cnr.isti.hpclab.matching.structures.output;

import java.io.IOException;

import it.cnr.isti.hpclab.matching.structures.SearchRequest;

public class NullResultOutput extends ResultOutput
{
	/** Public constructor */
	public NullResultOutput()
	{
		// Empty method
	}
		
	/** {@inheritDoc} */
	@Override
	public void print(final SearchRequest rq) throws IOException 
	{
		// Empty method
	}

	/** {@inheritDoc} */
	@Override
	public void close() throws IOException 
	{
		// Empty method
	}		
}
