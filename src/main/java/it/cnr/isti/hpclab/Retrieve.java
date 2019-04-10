package it.cnr.isti.hpclab;

import it.cnr.isti.hpclab.MatchingConfiguration.Property;

import java.io.IOException;
import java.util.Scanner;

import org.apache.log4j.Logger;
import org.terrier.utility.ApplicationSetup;

public class Retrieve 
{
	static {
		ApplicationSetup.setProperty("terrier.index.retrievalLoadingProfile.default","false");
	}

	public static void main(String args[]) throws IOException
	{	
		System.err.println("Performing retrieval with the following parameters:");
		for (Property p: Property.values())
			System.err.println(" -- " + p + " = " + MatchingConfiguration.get(p));
		
		System.err.println(" -> logger level = " + Logger.getRootLogger().getLevel());
	
		ApplicationSetup.TERRIER_INDEX_PATH = MatchingConfiguration.get(Property.INDEX_PATH);
		ApplicationSetup.TERRIER_INDEX_PREFIX = MatchingConfiguration.get(Property.INDEX_PREFIX);
		
		if (!(args.length == 1 && args[0].toLowerCase().equals("y"))) {
			Scanner input = new Scanner( System.in );
			String answer = null;
		
			do {
				System.out.print( "Continue [Y/n]? " );
				answer = input.nextLine();
			} while (!answer.matches("[y|Y|n|N|\n]"));
		
			input.close();
		
			if (answer.matches("[N|n]"))
				System.exit(0);
		}
				
		Querying querying = new Querying();
		querying.processQueries();
		querying.close();		
	}
}