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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Test;

public class TopQueueTest 
{
	@Test public void test()
	{
		Result res1 = new Result(1, 3.0f);
		Result res2 = new Result(2, 5.0f);
		
		TopQueue queue = new TopQueue(2);
		queue.insert(res1);
		assertEquals(queue.threshold(), 0.0, 0.00001);
		queue.insert(res2);
		assertEquals(queue.threshold(), res1.getScore(), 0.00001);
	}
	
	@Test public void serialization_0() throws IOException, ClassNotFoundException
	{
		TopQueue original_queue, copied_queue;
		
		original_queue = new TopQueue(0);
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(0, 6.66f);
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
	}

	@Test public void serialization_1() throws IOException, ClassNotFoundException
	{
		TopQueue original_queue, copied_queue;
		
		original_queue = new TopQueue(1);
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(1, 6.66f);
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(1);
		original_queue.insert(new Result(1, 1.1f));
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(1, 6.66f);
		original_queue.insert(new Result(1, 1.1f));
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(1, 6.66f);
		original_queue.insert(new Result(1, 7.1f));
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
        
		original_queue = new TopQueue(1, 6.66f);
		original_queue.insert(new Result(1, 7.1f));
		original_queue.insert(new Result(1, 6.1f));
		copied_queue = copy(original_queue);
        compare(original_queue, copied_queue);
	}

	private TopQueue copy(TopQueue original_queue) throws IOException, ClassNotFoundException 
	{
		/*
		TopQueue copied_queue;
		
		//Serialization of object
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(bos);
        out.writeObject(original_queue);
 
        //De-serialization of object
        ByteArrayInputStream bis = new   ByteArrayInputStream(bos.toByteArray());
        ObjectInputStream in = new ObjectInputStream(bis);
        copied_queue = (TopQueue) in.readObject();
        
        return copied_queue;
		*/
		return original_queue.copy();
		
	}

	private static void compare(TopQueue q1, TopQueue q2) 
	{
		assertEquals(q1.hashCode(), q2.hashCode());
		assertEquals(q1, q2);
		assertEquals(q1.threshold(), q2.threshold(), 1e-6);
		assertEquals(q1.size(), q2.size());
		
		Result r1, r2;
		while (!q1.top().isEmpty() && ! q2.top().isEmpty()) {
			r1 = q1.top().dequeue();
			r2 = q2.top().dequeue();
			assertEquals(r1.getDocId(), r2.getDocId());
			assertEquals(r1.getScore(), r2.getScore(), 1e-6);
		}
		
		assertTrue(q1.isEmpty());
		assertTrue(q2.isEmpty());
		
	}
}
