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
import it.cnr.isti.hpclab.maxscore.structures.MaxScoreIndex;
import it.unimi.dsi.logging.ProgressLogger;

public class MSGenerator {
	protected static Logger LOGGER = LoggerFactory.getLogger(MSGenerator.class);
	protected final static ProgressLogger pl = new ProgressLogger(LOGGER, 30, TimeUnit.SECONDS, "term");

	private final int num_terms;

	public static final class MSArgs 
	{
		// required arguments

		@Option(name = "-index", metaVar = "[String]", required = true, usage = "Input Index")
		public String index;

		@Option(name = "-wm", metaVar = "[String]", required = true, usage = "Weighting Model")
		public String wm_name;

		// optional arguments

		@Option(name = "-p", metaVar = "[Number]", required = false, usage = "Parallelism degree")
		public String parallelism;
	}

	public MSGenerator(final String src_index_path, final String src_index_prefix, final String wm_name)
			throws Exception {
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
		if (Files.exists(
				Paths.get(src_index_path + File.separator + src_index_prefix + MaxScoreIndex.USUAL_EXTENSION))) {
			throw new IllegalArgumentException(
					"Index directory " + src_index_path + " already contains an index with prefix " + src_index_prefix);
		}

		// check wm exists
		try {
			@SuppressWarnings("unused")
			WeightingModel mModel = (WeightingModel) (Class.forName(wm_name).asSubclass(WeightingModel.class)
					.getConstructor().newInstance());
		} catch (Exception e) {
			throw new IllegalArgumentException("Problem loading weighting model (" + wm_name + ")");
		}
	}

	public static class Command extends org.terrier.applications.CLITool.CLIParsedCLITool {

		protected Options getOptions() {
			Options options = super.getOptions();
			options.addOption(org.apache.commons.cli.Option.builder("w").argName("wmodel").hasArgs()
					.desc("weighting model").required().build());
			options.addOption(
					org.apache.commons.cli.Option.builder("p").argName("parallel").desc("Parallelism degree").build());
			return options;
		}

		@Override
		public int run(CommandLine line) throws Exception {
			MSArgs args = new MSArgs();
			if (line.hasOption("w"))
				args.wm_name = line.getOptionValue("w");
			args.index = ApplicationSetup.TERRIER_INDEX_PATH + "/" + ApplicationSetup.TERRIER_INDEX_PREFIX;
			if (line.hasOption("p"))
				args.parallelism = line.getOptionValue("p");
			execute(args);
			return 0;
		}

		@Override
		public String commandname() {
			return "micro-ms-generator";
		}

		@Override
		public String helpsummary() {
			return "generates a maxscore datastructure";
		}

	}

	public static void main(String[] argv)
	{
		MSArgs args = new MSArgs();
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
	
	public static void execute(MSArgs args) {

		IndexOnDisk.setIndexLoadingProfileAsRetrieval(false);
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