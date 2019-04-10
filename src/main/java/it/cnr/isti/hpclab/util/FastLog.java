package it.cnr.isti.hpclab.util;

import java.util.Random;

/**
 * 	Implementation of the ICSILog algorithm as described in O. Vinyals, G. Friedland, N. Mirghafori
 *	"Revisiting a basic function on current CPUs: A fast logarithm implementation with adjustable accuracy" (2007).
 *	(http://www.icsi.berkeley.edu/pubs/techreports/TR-07-002.pdf)
 */
public class FastLog 
{
	public static final double LN2  = Math.log( 2 );
	public static final double LN10 = Math.log( 10 );
	
	/**
	 *  Calculate the logarithm with base 2.
	 *
	 *  @param  val	the input value
	 *  @return the log2 of the value
	 */
	private static double _log2( double val )
	{
		return Math.log(val) / LN2;
	}
	
	private final int		q, qM1;
	private final float[]	data;
	private float			korr;
	
	/**
	 * 	Create a new logarithm calculation instance. This will
	 * 	hold the pre-calculated log values for a given base
	 * 	and a table size depending on a given mantissa quantization.
	 * 
	 *	@param	base	the logarithm base
	 *	@param	q		the quantization, the number of bits to remove
	 *					from the mantissa. For q = 11, the table storage
	 *					requires 32 KB.
	 */
	public FastLog( double base, int q )
	{
		final int tabSize = 1 << (24 - q);

		this.q	  = q;
		this.qM1  = q - 1;
		this.korr = (float) (LN2 / Math.log( base ));
		this.data = new float[ tabSize ];
		
		for ( int i = 0; i < tabSize; i++ ) {
			// note: the -150 is to avoid this addition in the calculation of the exponent (see the floatToRawIntBits doc).
			data[i] = (float) (_log2( i << q ) - 150);
		}
	}
	
	/**
	 *	Calculate the logarithm to the base given in the constructor.
	 *
	 *	@param	x	the argument. Must be positive! No boundary check!
	 *	@return		log(x)
	 */
	public float log( float x )
	{
		final int raw  = Float.floatToIntBits(x);
		final int exp  = (raw >> 23) & 0xFF;
		final int mant = (raw & 0x7FFFFF);

		return (exp + data[ exp == 0 ?
						    (mant >> qM1) :
							((mant | 0x800000) >> q) ]) * korr;
	}
	
    public static void main(final String[] args) 
    {
    	FastLog icsi = new FastLog(Math.E, 0);
    	final int total = 100_000_000;
    	Random rnd = new Random();
    	
    	final float test[] = new float[total];
    	for (int i = 0; i < total; i++)
    		test[i] = 1000 * rnd.nextFloat();
    	
    	long before;
    	long after;
    	int i;

    	while (true) {	       
            before = System.currentTimeMillis();
            i = total;
            float sum = 0.0f;
            while (--i >= 0) {
                sum += icsi.log(1.0f+test[i]);
            }
            after = System.currentTimeMillis();
            double t_approx = ((after - before) / 1000.0);
            System.out.println("" + t_approx + " sec, sum=" + sum + " approx");            

            before = System.currentTimeMillis();

            // only run 1/100th time as often because life is short
            i = total / 100;
            //i = total;
            sum = 0.0f;
            while (--i >= 0) {
                sum += Math.log(1.0f+test[i]);
            }
            after = System.currentTimeMillis();
            double t_original = ((after - before) / 10.0);
            System.out.println("" + t_original + " sec, sum=" + sum + " origin");

            double speedup = (t_original) / (t_approx);
            System.out.println("Speedup: " + speedup);

        }
    }
}
