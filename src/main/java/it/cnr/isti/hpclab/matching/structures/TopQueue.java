package it.cnr.isti.hpclab.matching.structures;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayPriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

/**
 * This is a priority queue with a fixed capacity to store candidate results in increasing order of score,
 * such as the head of the queue is the last top k candidate, the one with the lowest score (called threshold score) and the next to
 * be removed once a new candidate is inserted in a full queue.
 * 
 * @author Nicola Tonellotto
 */
@SuppressWarnings("serial")
public class TopQueue implements java.io.Serializable
{
	private static final int INTERNAL_MIN_QUEUE_SIZE = 20;
	private PriorityQueue<Result> queue = null;
	private int k;	
	
	/* The minimum score for insertion in a queue that is not yet full */
	private float threshold;
	
	/**
	 * Constructor.
	 * 
	 * @param k the size of the max heap, i.e., the number of elements to store in the queue
	 */
	public TopQueue(final int k)
	{
		this(k, 0.0f);
	}
	
	/**
	 * Constructor. 
	 * 
	 * @param k the size of the max heap, i.e., the number of elements to store in the queue
	 * @param th the initial value of the max heap threshold score
	 */
	public TopQueue(final int k, final float th)
	{
		this.k = k > 0 ? k : 1;
		this.queue = this.k <= INTERNAL_MIN_QUEUE_SIZE ? new ObjectArrayPriorityQueue<Result>(this.k) : new ObjectHeapPriorityQueue<Result>(this.k);
		this.threshold = th;		
	}

	/**
	 * Insert a new result in the queue, if there is space left or if it beats the current threshold
	 * 
	 * @param c the result to try to add to the max heap.
	 * @return true if the candidate beats the current threshold, false otherwise.
	 */
	public boolean insert(Result c)
	{
		if (c.getScore() > threshold) { // if c must enter
			if (queue.size() >= k) // if we have no space
				queue.dequeue();
			queue.enqueue(c); // c enters
			if (queue.size() >= k)
				threshold = Math.max(threshold, queue.first().getScore()); // threshold is updated if queue is full
			return true;
		}
		return false;
	}
	
	/**
	 * Returns the current threshold.
	 * @return
	 */
	public float threshold()
	{
		return threshold;
	}
	
	/**
	 * Check if the result would enter into the max heap, according to the current threshold,
	 * i.e., if its score is strictly greater than the current threshold.
	 * No actual insertion is performed.
	 * 
	 * @param c the result to check against
	 * @return true if the result would enter the max heap, false otherwise.
	 */
	public boolean wouldEnter(Result c)
	{
		return c.getScore() > threshold;
	}

	/**
	 * Check if the the score argument is strictly greater than the current threshold.
	 * 
	 * @param score the score to check
	 * @return true if the score is greater than the current threshold, false otherwise.
	 */
	public boolean wouldEnter(float score)
	{
		return score > threshold;
	}

	/**
	 * Return the max heap.
	 * Note: any modification will impact any running algorithm, since no deep copy is performed.
	 * 
	 * @return the current heap.
	 */
	public PriorityQueue<Result> top()
	{
		return this.queue;
	}
	
	/**
	 * Return the number of results in the max heap.
	 * @return the number of results in the max heap.
	 */
	public int size()
	{
		return queue.size();
	}
	
	/**
	 * Return true if the max heap is empty.
	 * @return true if the max heap is empty, false otherwise.
	 */
	public boolean isEmpty()
	{
		return queue.isEmpty();
	}
	
	/**
	 * Empties the current max heap. Leaves the minimum threshold unchanged.
	 */
	public void clear()
	{
		queue.clear();
	}

	/**
	 * Empties the current max heap. Reset the minimum threshold.
	 * 
	 *  @param new_threshold the new value of the (now empty) heap threshold.
	 */
	public void clear(final float new_threshold)
	{
		queue.clear();
		this.threshold = new_threshold;
	}

	@Override
	public String toString()
	{
		StringBuffer buf = new StringBuffer("< " + queue.size() + " items, " + threshold + ">");
		return buf.toString();
	}
	
	private void writeObject(java.io.ObjectOutputStream s) throws java.io.IOException 
	{
		s.defaultWriteObject();
		s.writeInt(k);
		s.writeFloat(threshold);
		s.writeObject(queue);
	}
	
	@SuppressWarnings("unchecked")
	private void readObject(java.io.ObjectInputStream s) throws java.io.IOException, ClassNotFoundException 
	{
		s.defaultReadObject();
		k = s.readInt();
		threshold = s.readFloat();
		if (k <= INTERNAL_MIN_QUEUE_SIZE)
			queue = (ObjectArrayPriorityQueue<Result>)s.readObject();
		else 
			queue = (ObjectHeapPriorityQueue<Result>)s.readObject();
	}
	
	/**
	 * Performs a deep copy of the top queue using in-memory serialization.
	 * 
	 * @return A new top queue identical to this one.
	 * 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 */
	public TopQueue copy() throws IOException, ClassNotFoundException
	{
		TopQueue copy = null;
		
		FastByteArrayOutputStream fbos = new FastByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(fbos)) {
        	out.writeObject(this);
        }

        FastByteArrayInputStream fbis = new FastByteArrayInputStream(fbos.array);
        try (ObjectInputStream in = new ObjectInputStream(fbis)) {
        	copy = (TopQueue) in.readObject();
        }
        
		return copy;
	}

	/**
	 * Highly inefficient, use with care.
	 */
	@Override
	public int hashCode() 
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + k;
		result = prime * result + Float.floatToIntBits(threshold);
		TopQueue copy;
		try {
			copy = copy();
			while (!copy.isEmpty())
				result = prime * result + copy.top().dequeue().hashCode();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return result;
	}

	/**
	 * Highly inefficient, use with care.
	 */

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		TopQueue that = (TopQueue) obj;
		if (k != that.k)
			return false;
		if (queue == null) {
			if (that.queue != null)
				return false;
			return true;
		}
		try {
			if (!TopQueue.compare(this.copy(), that.copy()))
				return false;
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		return true;
	}

	private static boolean compare(TopQueue tq1, TopQueue tq2) 
	{
		while (!tq1.top().isEmpty() && ! tq2.top().isEmpty())
			if (!tq1.top().dequeue().equals(tq2.top().dequeue()))
				return false;
		if (tq1.top().isEmpty() && tq2.top().isEmpty())
			return true;
		
		return false;
	}
}