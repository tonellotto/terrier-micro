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

package it.cnr.isti.hpclab.util;

public class StatsLine 
{
	private boolean first;
	
	private StringBuffer buf;
	
	public StatsLine()
	{
		first = true;
		buf = new StringBuffer();
		buf.append('{');
	}
	
	public StatsLine add(String key, String value)
	{
		// return this.add(key, value, true);
		return this.add(key, value, false);
	}
	
	public StatsLine add(String key, Number value)
	{
		return add(key, value.toString(), false);
	}

	public StatsLine add(String key, int value)
	{
		return add(key, Integer.toString(value), false);
	}
	
	public StatsLine add(String key, float value)
	{
		return add(key, Float.toString(value), false);
	}

	public StatsLine add(String key, double value)
	{
		return add(key, Double.toString(value), false);
	}
	
	public StatsLine add(String key, String[] values)
	{
		if (!first) {
			buf.append(", ");
		} else {
			first = false;
		}

		buf.append('"' + key + '"');
		buf.append(": [");

		for (int i = 0; i < values.length; ++i) {
			buf.append('"' + values[i] + '"');
			if (i != values.length - 1)
				buf.append(", ");
		}
		buf.append(']');

		return this;
	}

	public StatsLine add(String key, Number[] values)
	{
		if (!first) {
			buf.append(", ");
		} else {
			first = false;
		}

		buf.append('"' + key + '"');
		buf.append(": [");

		for (int i = 0; i < values.length; ++i) {
			buf.append(values[i]);
			if (i != values.length - 1)
				buf.append(", ");
		}
		buf.append(']');

		return this;
	}

	public StatsLine add(String key, String value, boolean escapeString)
	{
		if (!first) {
			buf.append(", ");
		} else {
			first = false;
		}

		buf.append('"' + key + '"');
		buf.append(": ");
		if (escapeString)
			buf.append('"' + value + '"');
		else
			buf.append(value);

		return this;
	}
	
	@Override
	public String toString()
	{
		return buf.toString() + '}';
	}
	
	public void reset()
	{
		first = true;
		buf.setLength(0);
		buf.append('{');
	}
	
	public void print()
	{
		System.out.println(this.toString());
	}
}
