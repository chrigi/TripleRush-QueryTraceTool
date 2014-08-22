package ch.ba.qdict.dictionary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import ch.ba.qdict.file.DictionaryWriter;
import ch.ba.qdict.graph.QueryGraph;
import ch.ba.qdict.graph.TPProcessor;

public class LookupTableCreator {

	public static String createLookupTable(String dictFileName, String metisFilePath, int noNodes,
			QueryGraph<String> traceGraph, String outPath) {

		System.out.println("Creating LookupTable...");
		long dictCreateStartTime = System.nanoTime();

		Map<String, Integer> lookupTable = new LinkedHashMap<String, Integer>();
		
		// Re-define METIS partition numbers to match natural node number of node vertices
		Map<Integer, Integer> partitionDef = new LinkedHashMap<Integer, Integer>();

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(metisFilePath + ".part." + Integer.toString(noNodes)));

			String partitionNumber = "";

			int i = 0;
			for (Map.Entry<String, Map<String, Integer>> indexVertex : traceGraph.getTraceGraph().entrySet()) {
				if (((partitionNumber = in.readLine()) != null)) {
					if (i >= noNodes) { // Skip first lines representing node vertices
						
						int natNode = TPProcessor.getNodeNumber(indexVertex.getKey(), noNodes);
						int metisNode = partitionDef.get(Integer.parseInt(partitionNumber));

						// Only add TPs to lookup table if METIS node is not equal to natural node
						if (metisNode != natNode) {
							lookupTable.put(indexVertex.getKey(), metisNode);
						}
					} else {
						partitionDef.put(Integer.parseInt(partitionNumber), Integer.parseInt(indexVertex.getKey()));
						++i;
					}
				} else {
					System.err
							.println("\tWARNING! Graph appears to have more vertices than METIS output! Vertex not in output: "
									+ indexVertex.getKey());
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

		long dictCreateEndTime = System.nanoTime();
		long dictCreateTime = dictCreateEndTime - dictCreateStartTime;

		System.out.println("\t#Entries in table: " + lookupTable.size());
		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(dictCreateTime, TimeUnit.NANOSECONDS)
				+ " ms");

		return DictionaryWriter.writeDictToFile(lookupTable, dictFileName, "tables", outPath);
	}
}
