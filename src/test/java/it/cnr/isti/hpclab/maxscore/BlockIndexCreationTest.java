package it.cnr.isti.hpclab.maxscore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import it.cnr.isti.hpclab.ef.EliasFano;
import it.cnr.isti.hpclab.ef.Generator;
import it.cnr.isti.hpclab.matching.MatchingSetupTest;
import it.cnr.isti.hpclab.maxscore.structures.Block;
import it.cnr.isti.hpclab.maxscore.structures.BlockEnumerator;
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terrier.structures.IndexOnDisk;
import org.terrier.utility.ApplicationSetup;

public class BlockIndexCreationTest extends MatchingSetupTest
{
	protected static IndexOnDisk originalIndex = null;
	protected static IndexOnDisk efIndex = null;
	
	private static int blockSize = 4;
	
	// private static String terms[] = { "abat", "ear", "room",  "st", "zeal"};
	private static int termids[]  = {      0,  610,    1723,  1929,   2356};
	private static int entries[]  = { 	   1,    6,       8,     9,      1};
	
	// private static int termids[]  = { 0, 1, 2, 3};
/*
DOCIDS: 
			21 
			1 2 8 14 15 21 
			2 4 6 10 12 15 17 19 
			1 4 5 10 12 15 18 19 21 
			21 
FREQUENCY:
			1 
			1 1 1 1 1 3 
			1 1 1 1 1 1 1 1 
			1 1 1 1 1 1 1 6 1 
			1 
SCORES
			1.6259019 
			0.77601004 0.9061236 1.1806028 0.9220619 0.5682368 1.0973719 
			0.51950026 0.6989017 0.7939922 0.6145792 0.57026565 0.32578248 0.613325 0.25002968 
			0.29272276 0.45984018 0.25813946 0.40436044 0.3752045 0.21434754 0.39705297 0.4782221 0.21458015 
			1.6259019 
 */
	private static long block_docids[][] = {
		{21},
		{14, 21},
		{10,19},
		{10,19,21},
		{21}
	};
	
	private static double block_maxscores[][] = {
			{ 1.6259019 }, 
			{ 1.1806028, 1.0973719 }, 
			{ 0.7939922, 0.613325 }, 
			{ 0.45984018, 0.4782221, 0.21458015 }, 
			{ 1.6259019 }
	};
	
	@BeforeClass public static void createIndexesOnDisk() throws Exception
	{
		makeEnvironment();
		doShakespeareIndexing();
		
		String args[] = {"-path", ApplicationSetup.TERRIER_INDEX_PATH, 
				 "-prefix", ApplicationSetup.TERRIER_INDEX_PREFIX + EliasFano.USUAL_EXTENSION, 
				 "-index", ApplicationSetup.TERRIER_INDEX_PATH + File.separator + ApplicationSetup.TERRIER_INDEX_PREFIX + ".properties", 
				 "-p", Integer.toString(3)};
		System.setProperty(EliasFano.LOG2QUANTUM, "3");
		Generator.main(args);

		loadIndexes();
		createMaxScoreIndexes();
		createBlockMaxScoreIndexes();
	}
	
	private static void loadIndexes()
	{
		originalIndex = IndexOnDisk.createIndex();
		efIndex = IndexOnDisk.createIndex(originalIndex.getPath(), originalIndex.getPrefix() + EliasFano.USUAL_EXTENSION);
	}

	private static void createMaxScoreIndexes() throws IOException
	{
		if (Files.exists(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + MaxScoreIndex.USUAL_EXTENSION)))
			Files.delete(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + MaxScoreIndex.USUAL_EXTENSION));
		
		String argsMS1[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".properties",
						 "-wm", "BM25",
				 		 "-p", Integer.toString(3)};

		String argsMS2[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef.properties",
				 "-wm", "BM25",
		 		 "-p", Integer.toString(3)};

		MSGenerator.main(argsMS1);
		MSGenerator.main(argsMS2);
	}

	private static void createBlockMaxScoreIndexes() throws IOException
	{
		if (Files.exists(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT)))
			Files.delete(Paths.get(originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef" + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT));
		
		String argsBMW1[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".properties",
				 		 "-p", Integer.toString(3),
				 		 "-b", Integer.toString(blockSize)};

		String argsBMW2[] = {"-index", originalIndex.getPath() + File.separator + originalIndex.getPrefix() + ".ef.properties",
		 		 "-p", Integer.toString(3),
		 		 "-b", Integer.toString(blockSize)};

		BMWGenerator.main(argsBMW1);
		BMWGenerator.main(argsBMW2);
	}

	@AfterClass public static void clean() 
	{
		deleteTerrierEtc();
	}
		
	@Before public void openIndex() throws IOException
	{
		loadIndexes();
	}
	
	@After public void closeIndex() throws IOException
	{
		originalIndex.close();
		efIndex.close();
	}
	
	@SuppressWarnings("resource")
	@Test public void testOriginalWrite() throws IOException
	{
		FileChannel fc = new FileInputStream(new File(originalIndex.getPath(), originalIndex.getPrefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT)).getChannel();
		LongBuffer  buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asLongBuffer();

		// Pretty useless test
		assertEquals(buf.get(0), blockSize);
		fc.close();
	}

	@SuppressWarnings("resource")
	@Test public void testSuccinctWrite() throws IOException
	{
		FileChannel fc = new FileInputStream(new File(efIndex.getPath(), efIndex.getPrefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT)).getChannel();
		LongBuffer  buf = fc.map(FileChannel.MapMode.READ_ONLY, 0, fc.size()).asLongBuffer();

		// Pretty useless test
		assertEquals(buf.get(0), blockSize);
		fc.close();
	}

	@Test 
	public void testOriginalRead() throws IOException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException
	{
		IndexOnDisk index = originalIndex;
		BlockMaxScoreIndex bmsi = new BlockMaxScoreIndex(index);

		BlockEnumerator benum;
		
		benum = bmsi.get(termids[0]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[0].length, entries[0] / blockSize + (entries[0] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[0].length, benum.size());
		
		benum.next();
		assertEquals(benum.last(), block_docids[0][0]);
		assertEquals(benum.score(), block_maxscores[0][0], 1e-6);

		benum.move(99);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[1]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[1].length, entries[1] / blockSize + (entries[1] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[1].length, benum.size());
		
		benum.move(14);
		assertEquals(benum.last(), block_docids[1][0]);
		assertEquals(benum.score(), block_maxscores[1][0], 1e-6);

		benum.move(15);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(19);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(21);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(22);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[2]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[2].length, entries[2] / blockSize + (entries[2] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[2].length, benum.size());

		benum.move(22);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[3]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[3].length, entries[3] / blockSize + (entries[3] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[3].length, benum.size());

		benum.next();
		assertEquals(benum.last(), block_docids[3][0]);
		assertEquals(benum.score(), block_maxscores[3][0], 1e-6);

		benum.next();
		assertEquals(benum.last(), block_docids[3][1]);
		assertEquals(benum.score(), block_maxscores[3][1], 1e-6);

		benum.next();
		assertEquals(benum.last(), block_docids[3][2]);
		assertEquals(benum.score(), block_maxscores[3][2], 1e-6);

		benum = bmsi.get(termids[3]);
		benum.move(21);		
		assertEquals(benum.last(), block_docids[3][2]);
		assertEquals(benum.score(), block_maxscores[3][2], 1e-6);

	}

	@Test 
	public void testSuccinctRead() throws IOException
	{
		IndexOnDisk index = efIndex;
		BlockMaxScoreIndex bmsi = new BlockMaxScoreIndex(index);
		
		BlockEnumerator benum;
		
		benum = bmsi.get(termids[0]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[0].length, entries[0] / blockSize + (entries[0] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[0].length, benum.size());
		
		benum.next();
		assertEquals(benum.last(), block_docids[0][0]);
		assertEquals(benum.score(), block_maxscores[0][0], 1e-6);

		benum.move(99);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[1]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[1].length, entries[1] / blockSize + (entries[1] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[1].length, benum.size());
		
		benum.move(14);
		assertEquals(benum.last(), block_docids[1][0]);
		assertEquals(benum.score(), block_maxscores[1][0], 1e-6);

		benum.move(15);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(19);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(21);
		assertEquals(benum.last(), block_docids[1][1]);
		assertEquals(benum.score(), block_maxscores[1][1], 1e-6);

		benum.move(22);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[2]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[2].length, entries[2] / blockSize + (entries[2] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[2].length, benum.size());

		benum.move(22);
		assertEquals(benum.last(), Block.END_OF_BLOCK);

		benum = bmsi.get(termids[3]);
		assertTrue(benum.hasNext());
		assertEquals(block_docids[3].length, entries[3] / blockSize + (entries[3] % blockSize == 0 ? 0 : 1));
		assertEquals(block_docids[3].length, benum.size());

		benum.next();
		assertEquals(benum.last(), block_docids[3][0]);
		assertEquals(benum.score(), block_maxscores[3][0], 1e-6);

		benum.next();
		assertEquals(benum.last(), block_docids[3][1]);
		assertEquals(benum.score(), block_maxscores[3][1], 1e-6);

		benum.next();
		assertEquals(benum.last(), block_docids[3][2]);
		assertEquals(benum.score(), block_maxscores[3][2], 1e-6);

		benum = bmsi.get(termids[3]);
		benum.move(21);		
		assertEquals(benum.last(), block_docids[3][2]);
		assertEquals(benum.score(), block_maxscores[3][2], 1e-6);
	}
}
