package ch.ba.qdict.metis;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import ch.ba.qdict.graph.QueryGraph;

public class METISFileCreator {
	
	public static String createLookupMetisFile(QueryGraph<String> traceGraph, String metisFileName, int noNodes, int noQueries, String outPath) {
		
		System.out.println("Creating METIS Input File...");
		
		long metisCreateStartTime = System.nanoTime();

		// Every vertex needs a number. ith vertex is represented by (i+1)th line in the input file. 1 based.
		Map<String, Integer> vertexNumberMap = traceGraph.getVertexNumberMap();
		
		int v = traceGraph.getNumberOfVertices();
		int m = traceGraph.getNumberOfEdges();

		File metisFile = new File(outPath + metisFileName + ".metis");

		BufferedWriter metisWriter = null;
		try {
			if (metisFile.exists()) {
				System.out.println("\tMETIS input file with name: " + metisFileName
						+ " already exists. Adding timestamp to new filename.");
				metisFile = new File(metisFile.getAbsolutePath() + "_" + System.currentTimeMillis() + ".metis");
			}
			metisFile.createNewFile();

			metisWriter = new BufferedWriter(new FileWriter(metisFile));

			// Format: #Vertices #Edges fmt ncon
			// fmt: 011 = No vert. size, has vert. weights, has edge weights
			// ncon: X = X vertex weights
			metisWriter.write(v + " " + m + " 011 " + (1 + noQueries));
			metisWriter.newLine();

			int i = 0;
			for (Map.Entry<String, Map<String, Integer>> vertex : traceGraph.getTraceGraph().entrySet()) {

				Map<String, Integer> edges = vertex.getValue();

				// Format for (i+1)th line:
				// vertex_weight_1 vertex_weight_2 ... dest_vertex_number_1 edge_weight_1 dest_vertex_number_2 edge_weight_2 ...

				if (i < noNodes) { // First noNodes entries are node vertices; Should be evenly partitioned over all partitions
					metisWriter.write(1 + " ");
					
					for (int j = 0; j < noQueries; j++) {
						metisWriter.write("0 ");
					}
				} else { // Index vertices should be partitioned multi-constrained
					metisWriter.write("0 ");
					
					Integer[] vertWeights = traceGraph.getWeightMap().get(vertex.getKey());

					for (Integer vWeight : vertWeights) {
						if (vWeight == null) {
							vWeight = new Integer(0);
						}
						metisWriter.write(vWeight.toString() + " ");
					}
				}

				// Write all edges from vertex
				for (Map.Entry<String, Integer> currEdge : edges.entrySet()) {
					int destVertexNumber = vertexNumberMap.get(currEdge.getKey());
					metisWriter.write(destVertexNumber + " " + currEdge.getValue() + " ");
				}

				if (++i < v) { // Be careful with new lines! Line = vertex!
					metisWriter.newLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				if (metisWriter != null) {
					metisWriter.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
		}

		long metisCreateEndTime = System.nanoTime();
		long metisCreateTime = metisCreateEndTime - metisCreateStartTime;

		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(metisCreateTime, TimeUnit.NANOSECONDS)
				+ " ms");

		return metisFile.getAbsolutePath();
	}

	public static String createDictMetisFile(QueryGraph<Integer> traceGraph, String metisFileName, int noNodes, int noQueries, String outPath) {

		System.out.println("Creating METIS Input File...");
		
		long metisCreateStartTime = System.nanoTime();

		// Every vertex needs a number. ith vertex is represented by (i+1)th line in the input file. 1 based.
		Map<Integer, Integer> vertexNumberMap = traceGraph.getVertexNumberMap();
		
		int v = traceGraph.getNumberOfVertices();
		int m = traceGraph.getNumberOfEdges();

		File metisFile = new File(outPath + metisFileName + ".metis");

		BufferedWriter metisWriter = null;
		try {
			if (metisFile.exists()) {
				System.out.println("\tMETIS input file with name: " + metisFileName
						+ " already exists. Adding timestamp to new filename.");
				metisFile = new File(metisFile.getAbsolutePath() + "_" + System.currentTimeMillis() + ".metis");
			}
			metisFile.createNewFile();

			metisWriter = new BufferedWriter(new FileWriter(metisFile));

			// Format: #Vertices #Edges fmt ncon
			// fmt: 011 = No vert. size, has vert. weights, has edge weights
			// ncon: X = X vertex weights
			metisWriter.write(v + " " + m + " 011 " + noQueries);
			metisWriter.newLine();

			int i = 0;
			for (Map.Entry<Integer, Map<Integer, Integer>> vertex : traceGraph.getTraceGraph().entrySet()) {

				Map<Integer, Integer> edges = vertex.getValue();

				// Format for (i+1)th line:
				// vertex_weight_1 vertex_weight_2 ... dest_vertex_number_1 edge_weight_1 dest_vertex_number_2 edge_weight_2 ...

				// Index vertices should be partitioned multi-constrained
				Integer[] vertWeights = traceGraph.getWeightMap().get(vertex.getKey());

				for (Integer vWeight : vertWeights) {
					if (vWeight == null) {
						vWeight = new Integer(0);
					}
					metisWriter.write(vWeight.toString() + " ");
				}

				// Write all edges from vertex
				for (Map.Entry<Integer, Integer> currEdge : edges.entrySet()) {
					int destVertexNumber = vertexNumberMap.get(currEdge.getKey());
					metisWriter.write(destVertexNumber + " " + currEdge.getValue() + " ");
				}

				if (++i < v) { // Be careful with new lines! Line = vertex!
					metisWriter.newLine();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(0);
		} finally {
			try {
				if (metisWriter != null) {
					metisWriter.close();
				}
			} catch (IOException ex) {
				ex.printStackTrace();
				System.exit(0);
			}
		}

		long metisCreateEndTime = System.nanoTime();
		long metisCreateTime = metisCreateEndTime - metisCreateStartTime;

		System.out.println("\tExecution time: " + TimeUnit.MILLISECONDS.convert(metisCreateTime, TimeUnit.NANOSECONDS)
				+ " ms");

		return metisFile.getAbsolutePath();
	}
}
