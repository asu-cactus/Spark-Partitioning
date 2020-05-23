package edu.asu.overheadanalysis.supergraph;

import org.apache.spark.sql.sources.In;

import java.lang.reflect.Array;
import java.util.*;

enum Color {
    RED,
    BLUE,
    PINK,
    YELLOW,
    GREEN,
    PURPLE,
    GOLD,
    ORANGE,
    BROWN,
    BLACK
}

class Node {
    HashMap<Color, Integer> adj;
    int pos;
    Color color;

    public Node(int m) {
        pos = m;
        adj = new HashMap<>();
    }

    public void merge(HashMap<Color, Integer> newAdj) {
        adj.putAll(newAdj);
    }

    public void setColor(Color color) {
        this.color = color;
    }

    public Color getColor() {
        return color;
    }

    public void print() {
        System.out.print(color.toString() + ": ");
        for (Color color : adj.keySet()) {
            if (adj.get(color) == 1)
                System.out.print(color.toString() + ", ");
        }
        System.out.print("\n");
    }

    public void setRandom(Color[] colorList) {
        Random random = new Random();
        boolean allZeros = true;
        for (int j = 0; j < pos; j++) {
            adj.put(colorList[j], random.nextInt(2));
            allZeros = adj.get(colorList[j]) == 0;
        }

        if (allZeros && pos > 0) {
            int j = random.nextInt(pos);
            adj.put(colorList[j], 1);
        }
    }
}

class Graph {
    public ArrayList<Node> nodes;
    public int size;

    public Graph(int size) {
        this.size = size;
        nodes = new ArrayList<Node>(size);
    }

    public static Graph mergeTwoGraphs(Graph a, Graph b) {
        int aCount = 0, bCount = 0;

        ArrayList<Node> nodes = new ArrayList<>();
        for (int i = 0; aCount < a.size || bCount < b.size; i++) {
            Node node = new Node(i);
            if (aCount < a.size && bCount < b.size) {

                if (a.nodes.get(aCount).getColor().equals(b.nodes.get(bCount).getColor())) {
                    node.setColor(a.nodes.get(aCount).getColor());
                    node.merge(a.nodes.get(aCount).adj);
                    node.merge(b.nodes.get(bCount).adj);
                    nodes.add(node);
                    aCount++;
                    bCount++;
                } else if (a.nodes.get(aCount).getColor().compareTo(b.nodes.get(bCount).getColor()) < 0) {
                    node.setColor(a.nodes.get(aCount).getColor());
                    node.merge(a.nodes.get(aCount).adj);
                    nodes.add(node);
                    aCount++;
                } else {
                    node.setColor(b.nodes.get(bCount).getColor());
                    node.merge(b.nodes.get(bCount).adj);
                    nodes.add(node);
                    bCount++;
                }

            } else if (aCount >= a.size) {
                node.setColor(b.nodes.get(bCount).getColor());
                node.merge(b.nodes.get(bCount).adj);
                nodes.add(node);
                bCount++;
            } else {
                node.setColor(a.nodes.get(aCount).getColor());
                node.merge(a.nodes.get(aCount).adj);
                nodes.add(node);
                aCount++;
            }
        }

        Graph newGraph = new Graph(nodes.size());
        newGraph.nodes = nodes;
        return newGraph;
    }

    public void newNode(Color color, int pos, int size) {
        Node node = new Node(pos);
        node.setColor(color);
        nodes.add(node);
    }

    public void mergeNode(int pos, Node node) {
        nodes.get(pos).merge(node.adj);
    }

    public void setRandom() {
        List<Color> colors = Arrays.asList(Color.values());
        Collections.shuffle(colors);
        Color[] colorList = new Color[size];
        colors.subList(0, size).toArray(colorList);

        for (int i = 0; i < size; i++) {
            Node node = new Node(i);
            node.setRandom(colorList);
            node.setColor(colorList[i]);
            nodes.add(node);
        }
    }

    public void print() {
        for (int i = 0; i < size; i++) {
            nodes.get(i).print();
        }
    }

    public void sort() {
        nodes.sort(new Comparator<Node>(){
            public int compare(Node i, Node j) {
                return i.getColor().compareTo(j.getColor());
            }
        });
    }
}

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
        int graph_count = 1000000;
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

//        graphs.get(0).print();
//        System.out.println("--------------------------------------");
//        graphs.get(1).print();
//        System.out.println("--------------------------------------");
//        Graph newGraph1 = Graph.mergeTwoGraphs(graphs.get(0), graphs.get(1));
//        newGraph1.print();
//        System.out.println("--------------------------------------");
        Graph newGraph = kWayMerge(graphs);
        newGraph.print();
    }
}
