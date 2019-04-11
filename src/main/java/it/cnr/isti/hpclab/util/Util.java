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

public class Util 
{	
	/** Join some strings together.
	  * @param in Strings to join
	  * @param join Character or String to join by */
    public static String JoinStrings(String[] in, String join) 
    {
    	if (in == null || in.length == 0)
            	return "";

    	StringBuilder s = new StringBuilder();
    	for (String t: in)
    		s.append(t).append(join);
    	
    	s.setLength(s.length() - join.length());
    	return s.toString();
    }
    
    public static String JoinStrings(String[] in)
    {
    	return Util.JoinStrings(in, "");
    }
    
    public static int IndexOfAny(String str, char[] searchChars) 
    {
        if (str == null || str.length() == 0 || searchChars == null || searchChars.length == 0)
            return -1;

        for (int i = 0; i < str.length(); i++) {
            char ch = str.charAt(i);
            for (int j = 0; j < searchChars.length; j++) {
                if (searchChars[j] == ch) {
                    return i;
                }
            }
        }
        return -1;
    }
    
    public static int IndexOfAny(String str, String[] searchStrs) 
    {
        if (str == null || str.length() == 0 || searchStrs == null || searchStrs.length == 0)
            return -1;

        int sz = searchStrs.length;

        // String's can't have a MAX_VALUEth index.
        int ret = Integer.MAX_VALUE;

        int tmp = 0;
        for (int i = 0; i < sz; i++) {
            String search = searchStrs[i];
            if (search == null) {
                continue;
            }
            tmp = str.indexOf(search);
            if (tmp == -1) {
                continue;
            }

            if (tmp < ret) {
                ret = tmp;
            }
        }

        return (ret == Integer.MAX_VALUE) ? -1 : ret;
    }
}
