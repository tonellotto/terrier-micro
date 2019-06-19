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

package it.cnr.isti.hpclab;

import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import java.io.IOException;
import java.util.Scanner;

import org.apache.commons.cli.CommandLine;
import org.apache.log4j.Logger;

public class Retrieve {
	
	public static class Command extends org.terrier.applications.CLITool.CLIParsedCLITool {

		@Override
		public int run(CommandLine line) throws Exception {
			Retrieve.main(line.getArgs());
			return 0;
		}

		@Override
		public String commandname() {
			return "micro-retrieve";
		}

		@Override
		public String helpsummary() {
			return "performs batch retrieval in terrier-micro";
		}

	}

	public static void main(String args[]) throws IOException
	{	
		System.err.println("Performing retrieval with the following parameters:");
		for (Property p: Property.values())
			System.err.println(" -- " + p + " = " + MatchingConfiguration.get(p));
		
		System.err.println(" -> logger level = " + Logger.getRootLogger().getLevel());
	
		org.terrier.utility.ApplicationSetup.TERRIER_INDEX_PATH = MatchingConfiguration.get(Property.INDEX_PATH);
		org.terrier.utility.ApplicationSetup.TERRIER_INDEX_PREFIX = MatchingConfiguration.get(Property.INDEX_PREFIX);
		
		if (!(args.length == 1 && args[0].toLowerCase().equals("y"))) {
			Scanner input = new Scanner( System.in );
			String answer = null;
		
			do {
				System.out.print( "Continue [Y/n]? " );
				answer = input.nextLine();
			} while (!answer.matches("[y|Y|n|N|\n]"));
		
			input.close();
		
			if (answer.matches("[N|n]"))
				Runtime.getRuntime().exit(0);
		}
		org.terrier.utility.ApplicationSetup.setProperty("terrier.index.retrievalLoadingProfile.default", "false");
		Querying querying = new Querying();
		querying.processQueries();
		querying.close();		
	}
}