package it.cnr.isti.hpclab.maxscore;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FilenameUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;

import it.cnr.isti.hpclab.ef.TermPartition;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;
import it.unimi.dsi.logging.ProgressLogger;

public class MSGenerator 
{
	protected static Logger LOGGER = LoggerFactory.getLogger(MSGenerator.class);
	protected final static ProgressLogger pl = new ProgressLogger(LOGGER, 30, TimeUnit.SECONDS, "term");
	
	private final int num_terms;
	
	public static final class Args 
	{
	    // required arguments

	    @Option(name = "-index",  metaVar = "[String]", required = true, usage = "Input Index")
	    public String index;

	    @Option(name = "-wm",  metaVar = "[String]", required = true, usage = "Weighting Model")
	    public String wm_name;

	    // optional arguments
	    
	    @Option(name = "-p", metaVar = "[Number]", required = false, usage = "Parallelism degree")
	    public String parallelism;	    
	}

	public MSGenerator(final String src_index_path, final String src_index_prefix, final String wm_name) throws Exception 
	{	
		// Load input index
		IndexOnDisk src_index = Index.createIndex(src_index_path, src_index_prefix);
		if (Index.getLastIndexLoadError() != null) {
			throw new IllegalArgumentException("Error loading index: " + Index.getLastIndexLoadError());
		}
		this.num_terms = src_index.getCollectionStatistics().getNumberOfUniqueTerms();
		src_index.close();
		LOGGER.info("Input index contains " + this.num_terms + " terms");
		pl.expectedUpdates = num_terms;
		
		// check dst maxscore index does not exist 
		if (Files.exists(Paths.get(src_index_path + File.separator + src_index_prefix + MaxScoreIndex.USUAL_EXTENSION))) {
			throw new IllegalArgumentException("Index directory " + src_index_path + " already contains an index with prefix " + src_index_prefix);
		}		
		
		// check wm exists
		try {
			@SuppressWarnings("unused")	WeightingModel mModel = (WeightingModel) (Class.forName(wm_name).asSubclass(WeightingModel.class).getConstructor().newInstance());
		} catch (Exception e) {
			throw new IllegalArgumentException("Problem loading weighting model (" + wm_name + ")");
		}
	}
	
	public static void main(String[] argv)
	{
		IndexOnDisk.setIndexLoadingProfileAsRetrieval(false);
		
		Args args = new Args();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(90));
		try {
			parser.parseArgument(argv);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		
		final String src_index_path = FilenameUtils.getFullPath(args.index);
		final String src_index_prefix = FilenameUtils.getBaseName(args.index);
		
		final String wm_name = (args.wm_name.indexOf('.') == -1) ? "it.cnr.isti.hpclab.matching.structures.model." + args.wm_name : args.wm_name;
		
		final int num_threads = ( (args.parallelism != null && Integer.parseInt(args.parallelism) > 1) 
										? Math.min(ForkJoinPool.commonPool().getParallelism(), Integer.parseInt(args.parallelism)) 
										: 1) ;
				
		LOGGER.info("Started " + MSGenerator.class.getSimpleName() + " with parallelism " + num_threads + " (out of " + ForkJoinPool.commonPool().getParallelism() + " max parallelism available)");
		LOGGER.warn("Multi-threaded MaxScore generation is experimental - caution advised due to threads competing for available memory! YMMV.");

		long starttime = System.currentTimeMillis();
		
		pl.start();
		try {
			MSGenerator generator = new MSGenerator(src_index_path, src_index_prefix, wm_name);
			float[] msa = new float[generator.num_terms];
			
			TermPartition[] partitions = generator.partition(num_threads);
			MSMapper mapper = new MSMapper(src_index_path, src_index_prefix, wm_name, msa);

			Arrays.stream(partitions).parallel().map(mapper).toArray(Object[]::new);
			
			long compresstime = System.currentTimeMillis();
			LOGGER.info("Parallel maxscore computation completed after " + (compresstime - starttime)/1000 + " seconds");
								
			ByteBuffer b_buf = ByteBuffer.allocate(msa.length * Float.BYTES);
			b_buf.asFloatBuffer().put(msa);

		    try (FileChannel out = FileChannel.open(Paths.get(src_index_path + File.separator + src_index_prefix + MaxScoreIndex.USUAL_EXTENSION), StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
		    	out.write(b_buf);
		    }
		    
			long writetime = System.currentTimeMillis();
			LOGGER.info("Sequential writing completed after " + (writetime - compresstime)/1000 + " seconds");
						
			writeProperties(src_index_path, src_index_prefix, wm_name);

			long endtime = System.currentTimeMillis();
			
			LOGGER.info("Multi-threaded MaxScore generation completed after " + (endtime - starttime)/1000 + " seconds, using "  + num_threads + " threads");
		} catch (Exception e) {
			e.printStackTrace();
		}
		pl.stop();
	}
	
	private static void writeProperties(String indexPath, String indexPrefix, String weightingModelClassName) throws IOException
	{
		IndexOnDisk index = Index.createIndex(indexPath, indexPrefix);
		index.setIndexProperty("index.maxscore.class","it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex");
		index.setIndexProperty("index.maxscore.parameter_types","org.terrier.structures.Index");
		index.setIndexProperty("index.maxscore.parameter_values","index");		
		index.setIndexProperty("index.maxscore.weighting_model", weightingModelClassName);

		index.close();
	}

	public TermPartition[] partition(final int num_threads)
	{
		return TermPartition.split(num_terms, num_threads);
	}
	
	synchronized public static void update_logger()
	{
		pl.update();
	}
}