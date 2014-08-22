package ch.ba.qdict;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ProcessBuilder.Redirect;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import ch.ba.qdict.dictionary.DictionaryCreator;
import ch.ba.qdict.dictionary.LookupTableCreator;
import ch.ba.qdict.file.TraceFileProcessor;
import ch.ba.qdict.graph.QueryGraph;
import ch.ba.qdict.metis.METISFileCreator;

public class TraceDictionary {

	public static void main(String[] args) {

		System.out.println("=== Trace Dictionary Creator ===");

		// --- Load Parameters -----------------------------------------------------------------------------------------------
		
		final Properties params = new Properties();
		InputStream input = null;

		try {
			input = new FileInputStream(args[0]);
			params.load(input);
			System.out.println("Launch Parameters:");
			System.out.println(params.toString());
		} catch (FileNotFoundException e) {
	        e.printStackTrace();
	        System.exit(0);
	    }catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}
		}

		String dataset = params.getProperty("DATASET");
		
		int queryIdMin = Integer.parseInt(params.getProperty("QUERY_ID_MIN"));
		int noQueries = Integer.parseInt(params.getProperty("NO_QUERIES"));

		int noNodes = Integer.parseInt(params.getProperty("NO_NODES", "4"));
		
		String outPath = params.getProperty("OUT_PATH");
		String traceFilesPath = params.getProperty("TRACES_PATH");
		String dictFilePath = params.getProperty("DICT_PATH");

		int traceWeight = Integer.parseInt(params.getProperty("TRACE_WEIGHT", "1000"));
		int nodeAffinityWeight = Integer.parseInt(params.getProperty("AFFINITY_WEIGHT", "100"));

		String metisBinaryPath = params.getProperty("METIS_BIN", "/home/user/ctschanz/usr/bin/gpmetis");
		
		// --- Variables -----------------------------------------------------------------------------------------------------
		
		String metisFileName = "";

		// --- Validate Input ------------------------------------------------------------------------------------------------

		if (queryIdMin < 0) {
			System.err.println("WARNING! Invalid query ID. Exiting.");
			System.exit(0);
		}
		
		// --- Create Lookup Table -------------------------------------------------------------------------------------------

		System.out.println("= Creating Lookup Table =");
		
		metisFileName = "qt-metis_" + dataset + "_table_" + noNodes;
		String tableFileName = "qt-table_" + dataset + "_" + noNodes;

		System.out.println("Generating Trace Graph");
		QueryGraph<String> traceGraphLookup = TraceFileProcessor.createLookupTraceGraph(traceFilesPath,
				traceWeight, nodeAffinityWeight, noNodes, dataset, noQueries, queryIdMin);

		if (traceGraphLookup != null) {
			System.out.println("Generating METIS Input File");
			String metisFilePath = METISFileCreator.createLookupMetisFile(traceGraphLookup, metisFileName, noNodes, noQueries, outPath);

			System.out.println("Running METIS");
			System.out.println("\tMETIS Execution output: ");
			runMetis(metisBinaryPath, metisFilePath, Integer.toString(noNodes));

			System.out.println("Generating Triple Pattern Lookup Table");
			String lookupFilePath = LookupTableCreator.createLookupTable(tableFileName, metisFilePath, noNodes,
					traceGraphLookup, outPath);

			System.out.println("Lookup Table Created: " + lookupFilePath);

			File metisInpFile = new File(metisFilePath);
			File metisOutFile = new File(metisFilePath + ".part." + Integer.toString(noNodes));
			metisInpFile.delete();
			metisOutFile.delete();
			
			traceGraphLookup.clear();
			traceGraphLookup = null;
		} else {
			System.err.println("Could not generate Trace Graph. Exiting.");
			System.exit(0);
		}
		
		// --- Create Dictionary ---------------------------------------------------------------------------------------------

		System.out.println("= Creating Dictionary =");
		
		metisFileName = "qt-metis_" + dataset + "_dict_" + noNodes;
		String dictFileName = "qt-dict_" + dataset + "_" + noNodes;
		String idMapFileName = "qt-idMap_" + dataset + "_" + noNodes;

		System.out.println("Generating Trace Graph");
		QueryGraph<Integer> traceGraphDict = TraceFileProcessor.createDictTraceGraph(traceFilesPath, 
				traceWeight, noNodes, dataset, noQueries, queryIdMin);

		if (traceGraphDict != null) {
			System.out.println("Generating METIS Input File");
			String metisFilePath = METISFileCreator.createDictMetisFile(traceGraphDict, metisFileName, noNodes, noQueries, outPath);

			System.out.println("Running METIS");
			System.out.println("\tMETIS Execution output: ");
			runMetis(metisBinaryPath, metisFilePath, Integer.toString(noNodes));

			System.out.println("Generating Dictionary");
			String newDictFilePath = DictionaryCreator.createDictionary(dictFileName, metisFilePath,
					dictFilePath + "normal-dict_" + dataset, traceGraphDict, noNodes, outPath, idMapFileName);

			System.out.println("Dictionary Created: " + newDictFilePath);

			File metisInpFile = new File(metisFilePath);
			File metisOutFile = new File(metisFilePath + ".part." + Integer.toString(noNodes));
			metisInpFile.delete();
			metisOutFile.delete();
			
			traceGraphDict.clear();
			traceGraphDict = null;
		} else {
			System.err.println("Could not generate Trace Graph. Exiting.");
			System.exit(0);
		}
		
		
		System.out.println("== DONE ==");
	}

	private static void runMetis(String metisBinaryPath, String metisFilePath, String noNodes) {

		ProcessBuilder metis = new ProcessBuilder(metisBinaryPath, metisFilePath, noNodes);
		metis.redirectOutput(Redirect.INHERIT);
		metis.redirectError(Redirect.INHERIT);

		try {
			System.out.println("");
			long metisStartTime = System.nanoTime();

			Process metisP = metis.start();
			metisP.waitFor();

			long metisEndTime = System.nanoTime();
			long metisTime = metisEndTime - metisStartTime;

			System.out.println("");
			System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(metisTime, TimeUnit.NANOSECONDS)
					+ " ms");
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		}
	}
}
