package edu.asu.overheadanalysis.supergraph;

import java.util.*;

public class SuperGraph {
  public static Graph kWayMerge(List<Graph> graphs) {
    while (graphs.size() > 1) {
      int i = 0;
      while (i < graphs.size() - 1) {
        Graph a = graphs.remove(0);
        Graph b = graphs.remove(0);
        graphs.add(Graph.mergeTwoGraphs(a, b));

        i += 2;
      }
    }
    return graphs.get(0);
  }

  public static void main(String[] args) {
    int graph_count = Integer.parseInt(args[0]);
    Random random = new Random();

    List<Graph> graphs = new ArrayList<>();
    for (int i = 0; i < graph_count; i++) {
      int size = random.nextInt(9) + 2;
      Graph graph = new Graph();
      graph.setRandom(size);
      graph.sort();
      graphs.add(graph);
    }

    //        graphs.get(0).print();
    //        System.out.println("--------------------------------------");
    //        graphs.get(1).print();
    //        System.out.println("--------------------------------------");
    long startTime = System.currentTimeMillis();
    Graph newGraph = kWayMerge(graphs);
    long endTime = System.currentTimeMillis();

    System.out.println(
        "Merging of " + graph_count + " took " + (endTime - startTime) + " milliseconds");
    newGraph.print();
  }
}
