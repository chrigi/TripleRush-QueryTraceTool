package ch.ba.qdict.graph;

import java.util.LinkedHashMap;
import java.util.Map;

public class QueryGraph<T> {
	
	private Map<T, Map<T, Integer>> traceGraph;
	private Map<T, Integer[]> weightMap;
	
	private int numberOfQueries;
	private boolean isUndirectedGraph;
	
	public QueryGraph(int numberOfQueries, boolean isUndirectedGraph) {
		this.traceGraph = new LinkedHashMap<>();
		this.weightMap = new LinkedHashMap<>();
		this.numberOfQueries = numberOfQueries;
		this.isUndirectedGraph = isUndirectedGraph;
	}

	public Map<T, Map<T, Integer>> getTraceGraph() {
		return traceGraph;
	}

	public Map<T, Integer[]> getWeightMap() {
		return weightMap;
	}
	
	public void addVertex(T vertex) {
		boolean containsVertex = traceGraph.containsKey(vertex);
		if (!containsVertex) {
			traceGraph.put(vertex, new LinkedHashMap<>());
		}
		
		boolean containsWeightVertex = weightMap.containsKey(vertex);
		if (!containsWeightVertex) {
			weightMap.put(vertex, new Integer[numberOfQueries]);
		}
	}
	
	public void addEdge(T source, T dest, int queryId, int weight, boolean sumWeights) {
		
		int edgeWeight = 0;
		
		// Update source -> dest Edge
		Map<T, Integer> sourceToDestEdges = traceGraph.get(source);
		edgeWeight = weight;
		
		if (sourceToDestEdges == null) {
			sourceToDestEdges = new LinkedHashMap<>();
		} else if (sumWeights) {
			Integer existingEdgeWeight = sourceToDestEdges.get(dest);
			if (existingEdgeWeight != null) {
				edgeWeight += existingEdgeWeight;
			}
		}
		
		sourceToDestEdges.put(dest, edgeWeight);
		traceGraph.put(source, sourceToDestEdges);
		
		// Update dest -> source Edge (if it's an undirected graph)
		if (isUndirectedGraph) {
			Map<T, Integer> destToSourceEdges = traceGraph.get(dest);
			edgeWeight = weight;
			
			if (destToSourceEdges == null) {
				destToSourceEdges = new LinkedHashMap<>();
			} else if (sumWeights) {
				Integer existingEdgeWeight = destToSourceEdges.get(source);
				if (existingEdgeWeight != null) {
					edgeWeight += existingEdgeWeight;
				}
			}
			
			destToSourceEdges.put(source, edgeWeight);
			traceGraph.put(dest, destToSourceEdges);
		}
			
		// Update source vertex weight
		Integer[] sourceWeights = weightMap.get(source);
		if (sourceWeights == null) {
			sourceWeights = new Integer[numberOfQueries];
		}
		
		sourceWeights[queryId] = 1;
		weightMap.put(source, sourceWeights);
		
		//Update dest vertex weight
		Integer[] destWeights = weightMap.get(dest);
		if (destWeights == null) {
			destWeights = new Integer[numberOfQueries];
		}
		
		destWeights[queryId] = 1;
		weightMap.put(dest, destWeights);
	}
	
	public Map<T, Integer> getEdgesFrom(T vertex) {
		return traceGraph.get(vertex);
	}
	
	public int getEdgeWeightFromTo(T source, T dest) {
		Map<T, Integer> sourceEdges = traceGraph.get(source);
		return sourceEdges.get(dest);
	}
	
	public int getNumberOfEdges() {
		int numberOfEdges = 0;
		
		for (Map.Entry<T, Map<T, Integer>> vertex : traceGraph.entrySet()) {
			numberOfEdges += vertex.getValue().size();
		}
		
		if (isUndirectedGraph) {
			return numberOfEdges/2;
		} else {
			return numberOfEdges;
		}
	}
	
	public int getNumberOfVertices() {
		return traceGraph.size();
	}
	
	public Map<T, Integer> getVertexNumberMap() {
		
		Map<T, Integer> vertexNumberMap = new LinkedHashMap<>();
		int j = 1;
		
		for (Map.Entry<T, Map<T, Integer>> vertex : traceGraph.entrySet()) {
			vertexNumberMap.put(vertex.getKey(), j++);
		}
		
		return vertexNumberMap;
	}
	
	public String getProperties() {
		return "isUnderectedGraph: " + isUndirectedGraph + ", numberOfQueries: " + numberOfQueries;
	}
	
	public boolean isEmpty() {
		return traceGraph.isEmpty();
	}
	
	public void clear() {
		traceGraph.clear();
		weightMap.clear();
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		QueryGraph<?> other = (QueryGraph<?>) obj;
		if (traceGraph == null) {
			if (other.traceGraph != null)
				return false;
		} else if (!traceGraph.equals(other.traceGraph))
			return false;
		if (weightMap == null) {
			if (other.weightMap != null)
				return false;
		} else if (!weightMap.equals(other.weightMap))
			return false;
		return true;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((traceGraph == null) ? 0 : traceGraph.hashCode());
		result = prime * result + ((weightMap == null) ? 0 : weightMap.hashCode());
		return result;
	}

}
