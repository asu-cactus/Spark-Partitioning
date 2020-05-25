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
        int graph_count = 2;
        Random random = new Random();

        List<Graph> graphs = new ArrayList<>();
        int maxNodes = 0;
        for (int i = 0; i < graph_count; i++) {
            int size = random.nextInt(9) + 2;
            Graph graph = new Graph(size);

            if (size > maxNodes)
                maxNodes = size;

            graph.setRandom();
            graph.sort();
            graphs.add(graph);
        }

        graphs.get(0).print();
        System.out.println("--------------------------------------");
        graphs.get(1).print();
        System.out.println("--------------------------------------");
        Graph newGraph1 = Graph.mergeTwoGraphs(graphs.get(0), graphs.get(1));
        newGraph1.print();
        System.out.println("--------------------------------------");
        Graph newGraph = kWayMerge(graphs);
        newGraph.print();
    }
}
