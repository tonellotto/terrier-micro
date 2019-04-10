package it.cnr.isti.hpclab.util;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

public class Format 
{
	public static String toString(Number n, int decimals) 
	{
        NumberFormat format = DecimalFormat.getInstance(Locale.US);
        format.setGroupingUsed(false);
        format.setRoundingMode(RoundingMode.CEILING);
        format.setMinimumFractionDigits(0);
        format.setMaximumFractionDigits(decimals);
        return format.format(n);
    }
}
