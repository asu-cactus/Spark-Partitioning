package edu.asu.overheadanalysis.supergraph;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SuperGraph {
  private static final ExecutorService executorService = Executors.newFixedThreadPool(8);

  public static Graph kWayMerge(List<Graph> graphs)
      throws ExecutionException, InterruptedException {
    Collection<Future<Graph>> futures = new LinkedList<>();
    ArrayList<Graph> tempList = new ArrayList<>();

    while (graphs.size() > 1) {
      if (graphs.size() % 2 != 0) {
        tempList.add(graphs.remove(graphs.size() - 1));
      }

      for (int i = 0; i < graphs.size(); i += 2) {
        Graph a = graphs.get(i);
        Graph b = graphs.get(i + 1);
        futures.add(
          executorService.submit(
            () -> {
              return Graph.mergeTwoGraphs(a, b);
            }));
      }

      for (Future<Graph> future : futures) {
        tempList.add(future.get());
      }

      graphs = tempList;
      tempList = new ArrayList<>();
      futures.clear();
    }

    executorService.shutdown();
    return graphs.get(0);
  }

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    int graph_count = Integer.parseInt(args[0]);
    Random random = new Random();

    List<Graph> graphs = new ArrayList<>();
    int totalNodes = 0;
    for (int i = 0; i < graph_count; i++) {
      int size = random.nextInt(9) + 2;
      Graph graph = new Graph();
      graph.setRandom(size);
      graph.sort();
      graphs.add(graph);
      totalNodes += size;
    }

    int avgNodes = totalNodes / graph_count;

    //        graphs.get(0).print();
    //        System.out.println("--------------------------------------");
    //        graphs.get(1).print();
    //        System.out.println("--------------------------------------");
    long startTime = System.currentTimeMillis();
    Graph newGraph = kWayMerge(graphs);
    long endTime = System.currentTimeMillis();

    System.out.println(
        "Merging of "
            + graph_count
            + " DAGS with avg nodes: "
            + avgNodes
            + " took "
            + (endTime - startTime)
            + " milliseconds");
    newGraph.print();
  }
}
