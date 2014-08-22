package ch.ba.qdict.file;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

import ch.ba.qdict.graph.QueryGraph;
import ch.ba.qdict.graph.TPProcessor;

public class TraceFileProcessor {
	
	public static QueryGraph<String> createLookupTraceGraph(String traceFilesPath, int traceWeight,
			int nodeAffinityWeight, int noNodes, String dataset, int noQueries, int queryIdMin) {
		
		System.out.println("Processing Query Trace Files...");
		
		long traceReadStartTime = System.nanoTime();

		QueryGraph<String> traceGraph = new QueryGraph<String>(noQueries, true);
		System.out.println("\tQueryGraph properties: " + traceGraph.getProperties());
		int noTraces = 0;
		
		// Adding node vertices to query graph; ensures that they are the first noNodes entries in the traceGraph entrySet
		for(int i = 0; i < noNodes; i++) {
			traceGraph.addVertex(Integer.toString(i));
		}

		File tracesFolder = new File(traceFilesPath + dataset + "/" + noNodes + "_nodes/");
		System.out.println("\tTraces folder: " + tracesFolder.getAbsolutePath());
		for (File traceFile : tracesFolder.listFiles()) {

			System.out.println("\tReading File: " + traceFile.getName());

			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(traceFile));

				String line = "";
				while ((line = in.readLine()) != null) {

					// Trace format: queryId sourceTP destinationTP queryType
					// queryType = forwarding/redirecting
					String[] trace = line.split(" ");
					int query = Integer.parseInt(trace[0]);
					String source = trace[1];
					String dest = trace[2];
					
					// Adding source-dest Edge (sum weights if edge already exists)
					traceGraph.addEdge(source, dest, query - queryIdMin, traceWeight, true);
					
					// Adding source-node Edge (don't sum weights if edge already exists)
					traceGraph.addEdge(source, Integer.toString(TPProcessor.getNodeNumber(source, noNodes)), query - queryIdMin, nodeAffinityWeight, false);
					
					// Adding dest-node Edge (don't sum weights if edge already exists)
					traceGraph.addEdge(dest, Integer.toString(TPProcessor.getNodeNumber(dest, noNodes)), query - queryIdMin, nodeAffinityWeight, false);
					
					noTraces++;
				}

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}

		}

		long traceReadEndTime = System.nanoTime();
		long traceReadTime = traceReadEndTime - traceReadStartTime;

		System.out.println("\tProcessed traces: " + noTraces);
		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(traceReadTime, TimeUnit.NANOSECONDS)
				+ " ms");

		if (traceGraph.isEmpty()) {
			System.err.println("WARNING! No usable traces found.");
			return null;
		}
		
		return traceGraph;
	}
	
	public static QueryGraph<Integer> createDictTraceGraph(String traceFilesPath, int traceWeight, 
			int noNodes, String dataset, int noQueries, int queryIdMin) {

		System.out.println("Processing Query Trace Files...");
		
		long traceReadStartTime = System.nanoTime();

		QueryGraph<Integer> traceGraph = new QueryGraph<Integer>(noQueries, true);
		System.out.println("\tQueryGraph properties: " + traceGraph.getProperties());
		int noTraces = 0;
		int noIgnoredTraces = 0;

		File tracesFolder = new File(traceFilesPath + dataset + "/" + noNodes + "_nodes/");
		System.out.println("\tTraces folder: " + tracesFolder.getAbsolutePath());
		for (File traceFile : tracesFolder.listFiles()) {

			System.out.println("\tReading File: " + traceFile.getName());

			BufferedReader in = null;
			try {
				in = new BufferedReader(new FileReader(traceFile));

				String line = "";
				while ((line = in.readLine()) != null) {

					// Trace format: queryId sourceTP destinationTP queryType
					// queryType = forwarding/redirecting
					String[] trace = line.split(" ");
					int query = Integer.parseInt(trace[0]);
					int source = TPProcessor.getSigId(trace[1]);
					int dest = TPProcessor.getSigId(trace[2]);
					
					if (source != dest) { // Only add Edge if it crosses SigId boundary
						// Adding source-dest Edge (sum weights if edge already exists)
						traceGraph.addEdge(source, dest, query - queryIdMin, traceWeight, true);
						noTraces++;
					} else {
						noIgnoredTraces++;
					}
				}

			} catch (IOException e) {
				e.printStackTrace();
				System.exit(0);
			} finally {
				try {
					if (in != null) {
						in.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
					System.exit(0);
				}
			}

		}

		long traceReadEndTime = System.nanoTime();
		long traceReadTime = traceReadEndTime - traceReadStartTime;

		System.out.println("\tIgnored Traces: " + noIgnoredTraces);
		System.out.println("\tUsed Traces: " + noTraces);
		System.out.println("\tTotal Processed Traces: " + (noTraces + noIgnoredTraces));
		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(traceReadTime, TimeUnit.NANOSECONDS)
				+ " ms");

		if (traceGraph.isEmpty()) {
			System.err.println("WARNING! No usable traces found.");
			return null;
		}
		
		return traceGraph;
	}
}
