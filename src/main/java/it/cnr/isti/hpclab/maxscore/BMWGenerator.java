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

package it.cnr.isti.hpclab.maxscore;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.io.FilenameUtils;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terrier.structures.Index;
import org.terrier.structures.IndexOnDisk;
import org.terrier.utility.ApplicationSetup;

import it.cnr.isti.hpclab.ef.TermPartition;
import it.cnr.isti.hpclab.matching.structures.WeightingModel;
import it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex;
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;
import it.unimi.dsi.logging.ProgressLogger;

public class BMWGenerator 
{
	protected static Logger LOGGER = LoggerFactory.getLogger(BMWGenerator.class);
	protected final static ProgressLogger pl = new ProgressLogger(LOGGER, 30, TimeUnit.SECONDS, "term");
	
	private final int num_terms;
	private final String wm_name;
	
	public static final class BMWArgs 
	{
	    // required arguments

	    @Option(name = "-index",  metaVar = "[String]", required = true, usage = "Input Index")
	    public String index;

	    @Option(name = "-b",  metaVar = "[Number]", required = true, usage = "Block Size")
	    public String bs;

	    // optional arguments
	    
	    @Option(name = "-p", metaVar = "[Number]", required = false, usage = "Parallelism degree")
	    public String parallelism;	    
	}

	public static class Command extends org.terrier.applications.CLITool.CLIParsedCLITool {

		protected Options getOptions()
		{
			Options options = super.getOptions();
			options.addOption(org.apache.commons.cli.Option.builder("b")
					.argName("blocks")
					.hasArgs()
					.desc("Block Size")
					.required()
					.build());
			options.addOption(org.apache.commons.cli.Option.builder("p")
					.argName("parallel")
					.desc("Parallelism degree")
					.build());
			return options;
		}
		
		@Override
		public int run(CommandLine line) throws Exception {
			BMWArgs args = new BMWArgs();
			if (line.hasOption("b"))
				args.bs = line.getOptionValue("b");
			args.index = ApplicationSetup.TERRIER_INDEX_PATH + "/" + ApplicationSetup.TERRIER_INDEX_PREFIX;
			if (line.hasOption("p"))
				args.parallelism = line.getOptionValue("p");
			execute(args);
			return 0;
		}

		@Override
		public String commandname() {
			return "micro-bmw-generator";
		}

		@Override
		public String helpsummary() {
			return "generates a bmw datastructure";
		}
	}

	public BMWGenerator(final String src_index_path, final String src_index_prefix) throws Exception 
	{	
		// Load input index
		IndexOnDisk src_index = Index.createIndex(src_index_path, src_index_prefix);
		if (Index.getLastIndexLoadError() != null) {
			throw new IllegalArgumentException("Error loading index: " + Index.getLastIndexLoadError());
		}
		this.num_terms = src_index.getCollectionStatistics().getNumberOfUniqueTerms();
		this.wm_name = src_index.getIndexProperty("index.maxscore.weighting_model", "");
		
		src_index.close();
		LOGGER.info("Input index contains " + this.num_terms + " terms");
		pl.expectedUpdates = num_terms;
		
		// check dst maxscore index does exist
		if (!Files.exists(Paths.get(src_index_path + File.separator + src_index_prefix + MaxScoreIndex.USUAL_EXTENSION))) {
			throw new IllegalArgumentException("Index directory " + src_index_path + " does not contain a max score index with prefix " + src_index_prefix + ". Please generate one first.");
		}

		// check dst blockindex index does not exist 
		if (Files.exists(Paths.get(src_index_path + File.separator + src_index_prefix + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT))) {
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
		BMWArgs args = new BMWArgs();
		CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(90));
		try {
			parser.parseArgument(argv);
		} catch (Exception e) {
			System.err.println(e.getMessage());
			parser.printUsage(System.err);
			return;
		}
		execute(args);
	}
	
	public static void execute(BMWArgs args) {

		IndexOnDisk.setIndexLoadingProfileAsRetrieval(false);
		final String src_index_path = FilenameUtils.getFullPath(args.index);
		final String src_index_prefix = FilenameUtils.getBaseName(args.index);
		
		final int num_threads = ( (args.parallelism != null && Integer.parseInt(args.parallelism) > 1) 
										? Math.min(ForkJoinPool.commonPool().getParallelism(), Integer.parseInt(args.parallelism)) 
										: 1) ;

		final int block_size = ( (args.bs != null && Integer.parseInt(args.bs) >= 2) ? Integer.parseInt(args.bs) : 2) ;

		LOGGER.info("Started " + BMWGenerator.class.getSimpleName() + " with parallelism " + num_threads + " (out of " + ForkJoinPool.commonPool().getParallelism() + " max parallelism available), using posting blocks of size " + block_size);
		LOGGER.warn("Multi-threaded MaxScore generation is experimental - caution advised due to threads competing for available memory! YMMV.");

		long starttime = System.currentTimeMillis();
		
		pl.start();
		try {
			BMWGenerator generator = new BMWGenerator(src_index_path, src_index_prefix);
			
			TermPartition[] partitions = generator.partition(num_threads);
			BMWMapper mapper = new BMWMapper(src_index_path, src_index_prefix, generator.wm_name, block_size);
			BMWReducer merger = new BMWReducer(src_index_path, src_index_prefix);

			TermPartition[] tmp_partitions = Arrays.stream(partitions).parallel().map(mapper).sorted().toArray(TermPartition[]::new);
			
			TermPartition last_partition = Arrays.stream(tmp_partitions).reduce(merger).get();
			
			
			// Eventually, we rename the last merge
			Files.move(Paths.get(src_index_path, last_partition.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT),
					   Paths.get(src_index_path, src_index_prefix        + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.DOCID_EXT));
			Files.move(Paths.get(src_index_path, last_partition.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT),
					   Paths.get(src_index_path, src_index_prefix        + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.SCORE_EXT));
			Files.move(Paths.get(src_index_path, last_partition.prefix() + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT),
					   Paths.get(src_index_path, src_index_prefix        + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT));

			// important, last element is total number of blocks
			String os = src_index_path + File.separator + src_index_prefix + BlockMaxScoreIndex.STRUCTURE_NAME + BlockMaxScoreIndex.OFFSET_EXT;
			DataOutputStream offsetsOutput = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(os, true)));
			offsetsOutput.writeLong(Files.size(Paths.get(os)) / Long.BYTES);
			offsetsOutput.close();
						
			writeProperties(src_index_path, src_index_prefix, block_size);

			long endtime = System.currentTimeMillis();
			
			LOGGER.info("Multi-threaded MaxScore generation completed after " + (endtime - starttime)/1000 + " seconds, using "  + num_threads + " threads");
		} catch (Exception e) {
			e.printStackTrace();
		}
		pl.stop();
	}
	
	private static void writeProperties(String indexPath, String indexPrefix, final int block_size) throws IOException
	{
		IndexOnDisk index = Index.createIndex(indexPath, indexPrefix);
		index.setIndexProperty("index.blockmaxscore.class","it.cnr.isti.hpclab.maxscore.structures.BlockMaxScoreIndex");
		index.setIndexProperty("index.blockmaxscore.parameter_types","org.terrier.structures.Index");
		index.setIndexProperty("index.blockmaxscore.parameter_values","index");
		index.setIndexProperty("index.blockmaxscore.block_size",Integer.toString(block_size));
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