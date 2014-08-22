package ch.ba.qdict.dictionary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import ch.ba.qdict.file.DictionaryWriter;
import ch.ba.qdict.file.OldNewIdMapWriter;
import ch.ba.qdict.graph.QueryGraph;

public class DictionaryCreator {

	public static String createDictionary(String newDictFileName, String metisFilePath, String oldDictFilePath,
			QueryGraph<Integer> traceGraph, int noNodes, String outPath, String idMapFileName) {

		System.out.println("Creating Dictionary...");
		
		long dictCreateStartTime = System.nanoTime();

		Map<String, Integer> newDict = new LinkedHashMap<String, Integer>();
		Map<Integer, Integer> oldNewIdMap = new LinkedHashMap<Integer, Integer>();
		
		// Parse old dictionary to retrieve Strings
		List<String> oldDict = parseTRDict(oldDictFilePath);

		int[] sc = new int[noNodes];

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(metisFilePath + ".part." + Integer.toString(noNodes)));

			String metisNode = "";

			for (Map.Entry<Integer, Map<Integer, Integer>> indexVertex : traceGraph.getTraceGraph().entrySet()) {
				if (((metisNode = in.readLine()) != null)) {
					int vertId = indexVertex.getKey();
					if (vertId < oldDict.size()) {
						int mn = Integer.parseInt(metisNode);

						int node = (mn == 0) ? noNodes : mn;
						int newId = sc[mn] * noNodes + node;

						newDict.put(oldDict.get(vertId), newId);
						oldNewIdMap.put(vertId, newId);
						sc[mn] += 1;
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

		System.out.println("\t#Entries in table: " + newDict.size());
		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(dictCreateTime, TimeUnit.NANOSECONDS)
				+ " ms");

		System.out.println("Generating ID Map");
		System.out.println("ID Map Created: " + OldNewIdMapWriter.writeMapToFile(oldNewIdMap, idMapFileName, outPath));
		
		return DictionaryWriter.writeDictToFile(newDict, newDictFileName, "dicts", outPath);
	}

	private static List<String> parseTRDict(String dictFilePath) {

		List<String> trDict = new ArrayList<String>();

		BufferedReader in = null;
		try {
			in = new BufferedReader(new FileReader(dictFilePath));

			String line = "";
			while ((line = in.readLine()) != null) {
				trDict.add(line);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (in != null) {
					in.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		return trDict;
	}

}
