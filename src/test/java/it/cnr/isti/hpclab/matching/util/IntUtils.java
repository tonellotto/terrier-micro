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

package it.cnr.isti.hpclab.matching.util;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;

public class IntUtils 
{
	/**
	 * This class will generate "uniform" lists of random integers. 
	 * 
	 * Original code here: https://github.com/lemire/simplebitmapbenchmark/blob/master/src/bitmapbenchmarks/synth/UniformDataGenerator.java
	 */
	public static class UniformGenerator 
	{
		/**
		 * generates randomly n distinct integers (sorted) from min (inclusive) to max (exclusive).
		 */
		private int[] build(final int n, final int min, final int max) 
		{
			if (n > max - min)
				throw new IllegalArgumentException("n (" + n + ") can't be greater than range (" + (max - min) + ")");

			return ThreadLocalRandom.current().ints(min,  max).distinct().limit(n).sorted().toArray();
	    }
		
		/**
		 * Outputs all integers (sorted) from the range [min, max) that are not in the array x (sorted).
		 */
		private static int[] complement(final int[] x, final int min, final int max) 
		{
			int[] y = new int[max - min - x.length];
			int y_v = min;
			int y_pos = 0;
	        for (int x_pos = 0; x_pos < x.length; ++x_pos) {
	        	int x_v = x[x_pos];
	        	for (; y_v < x_v; ++y_v)
	        		y[y_pos++] = y_v;
	        	++y_v;
	        }
	        
	        while (y_pos < y.length)
	        	y[y_pos++] = y_v++;
	        return y;
	    }
		
		/**
		 * Generates randomly n distinct sorted integers from 0 (inclusive) to max (exclusive).
		 */
        public int[] generate(final int n, final int max) 
        {
        	return generate(n, 0, max);
        }
        
		/**
		 * Generates randomly n distinct sorted integers from min (inclusive) to max (exclusive).
		 */
        public int[] generate(final int n, final int min, final int max) 
        {
        	if (n > (max - min)/2) 
        		return complement( build ( (max - min) - n, min, max), min, max);
        	return build(n, min, max);
        }

	}
	
	/**
	 * This class will generate lists of random integers with a "clustered" distribution.
	 * 
	 * Reference: Anh VN, Moffat A. Index compression using 64-bit words. Software: Practice and Experience 2010; 40(2):131-147.
	 * 
	 * Original code here: https://github.com/lemire/simplebitmapbenchmark/blob/master/src/bitmapbenchmarks/synth/ClusteredDataGenerator.java
	 */
	public static class ClusteredGenerator
	{
		private final UniformGenerator unigen = new UniformGenerator();

		private void fillUniform(final int[] array, final int offset, final int length, final int min, final int max) 
		{
			int[] v = this.unigen.generate(length, min, max);
			System.arraycopy(v, 0, array, offset, v.length);
		}

		private void fillClustered(final int[] array, final int offset, final int length, final int min, final int max) 
		{
			final int range = max - min;
			if ((range == length) || (length <= 32)) {
				fillUniform(array, offset, length, min, max);
			} else {
				final int cut = length / 2 + ((range - length - 1 > 0) ? ThreadLocalRandom.current().nextInt(range - length - 1) : 0);
				final double p = ThreadLocalRandom.current().nextDouble();
				if (p < 0.25) {
					fillUniform(array, offset, length / 2, min, min + cut);
					fillClustered(array, offset + length / 2, length - length / 2, min + cut, max);
				} else if (p < 0.5) {
					fillClustered(array, offset, length / 2, min, min + cut);
					fillUniform(array, offset + length / 2, length - length / 2, min + cut, max);
				} else {
					fillClustered(array, offset, length / 2, min, min + cut);
					fillClustered(array, offset + length / 2, length - length / 2, min + cut, max);
				}
			}
		}

		/**
		 * Generates randomly n distinct integers (sorted) from 0 (inclusive) to max (exclusive).
		 */
		public int[] generate(final int n, final int max) 
		{
			return generate(n, 0, max);
		}
		
		/**
		 * Generates randomly n distinct integers (sorted) from min (inclusive) to max (exclusive).
		 */
		public int[] generate(final int n, final int min, final int max) 
		{
			int[] array = new int[n];
			fillClustered(array, 0, n, min, max);
			return array;
		}
	}
	
	public static int[] union(final int[] x1, final int[] x2) 
	{
	    if (0 == x1.length)
	    	return Arrays.copyOf(x2, x2.length);
	    
	    if (0 == x2.length)
	    	return Arrays.copyOf(x1, x1.length);

	    int[] buffer =  new int[x1.length + x2.length];

	    int buffer_pos = 0;
	    int x1_pos = 0;
	    int x2_pos = 0;
	    
	    while (true) {
	    	
	    	if (x1[x1_pos] < x2[x2_pos]) {
	    		buffer[buffer_pos++] = x1[x1_pos++];
	    		if (x1_pos >= x1.length) { // copy remaining elements from x2
	    			for (; x2_pos < x2.length; ++x2_pos)
	    				buffer[buffer_pos++] = x2[x2_pos];
	    			break;
	    		}
	    	} 
	    	
	    	else if (x1[x1_pos] == x2[x2_pos]) {
	    		buffer[buffer_pos++] = x1[x1_pos];
	    		++x1_pos;
	    		++x2_pos;
	    		if (x1_pos >= x1.length) { // copy remaining elements from x1
	    			for (; x2_pos < x2.length; ++x2_pos)
	    				buffer[buffer_pos++] = x2[x2_pos];
	    			break;
	    		}
	    		if (x2_pos >= x2.length) { // copy remaining elements from x2
	    			for (; x1_pos < x1.length; ++x1_pos)
	    				buffer[buffer_pos++] = x1[x1_pos];
	    			break;
	    		}
	    	}
	    	
	    	else { //if (x1[x1_pos] > x2[x2_pos]) {
	    		buffer[buffer_pos++] = x2[x2_pos++];
	    		if (x2_pos >= x2.length) { // copy remaining elements from x1
	    			for (; x1_pos < x1.length; ++x1_pos)
	    				buffer[buffer_pos++] = x1[x1_pos];
	    			break;
	    		}
	    	}
	    	
	    }
	    return Arrays.copyOf(buffer, buffer_pos);
	}
	
	public static int[] union(final int[]... x) 
	{
	    if (x.length == 0)
	    	throw new IllegalArgumentException("Input must contain at least an array");
	    
	    // sort arrays by length
	    Arrays.sort(x, (x1, x2) -> Integer.compare(x1.length, x2.length));
	    
	    int[] result = new int[0];
	    
	    for (int i = 0; i < x.length; ++i)
	    	result = union(result, x[i]);

	    return result;
	}

	public static int[] intersection(final int[] x1, final int[] x2) 
	{
	    if (0 == x1.length || 0 == x2.length)
	    	return new int[0];

		int[] buffer =  new int[Math.min(x1.length, x2.length)];
		
		int x1_pos = 0;
		int x2_pos = 0;
		int buffer_pos = 0;

		loop: while (true) {
			
			// advance x2
			while (x2[x2_pos] < x1[x1_pos]) {
				++x2_pos;
				if (x2_pos == x2.length)
					break loop;				
			}

			// advance x1
			while (x1[x1_pos] < x2[x2_pos]) {
				++x1_pos;
				if (x1_pos == x1.length)
					break loop;
			}
			
			// if found, copy and advance x1 and x2
			if (x1[x1_pos] == x2[x2_pos]) {
				buffer[buffer_pos++] = x1[x1_pos];
				++x1_pos;
				if (x1_pos == x1.length)
					break;
				++x2_pos;
				if (x2_pos == x2.length)
					break;
			}
		}
		return Arrays.copyOf(buffer, buffer_pos);
	}
	
	public static int[] intersection(final int[]... x) 
	{
	    if (x.length == 0)
	    	throw new IllegalArgumentException("Input must contain at least an array");
	    
	    // sort arrays by length
	    Arrays.sort(x, (x1, x2) -> Integer.compare(x1.length, x2.length));
	    
	    int[] result = x[0];
	    
	    for (int i = 1; i < x.length; ++i)
	    	result = intersection(result, x[i]);

	    return result;
	}	
	
}
