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
