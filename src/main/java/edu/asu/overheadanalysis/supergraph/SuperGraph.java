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
    int size;
    int pos;
    Color color;

    public void setColor(Color color) {
        this.color = color;
    }

    public Color getColor() {
        return color;
    }

    public Node(int m, int n) {
        pos = m;
        size = n;
        adj = new HashMap<>();
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
    ArrayList<Node> nodes;
    int size;

    public Graph(int size) {
        this.size = size;
        nodes = new ArrayList<Node>(size);
    }

    public void setRandom() {
        List<Color> colors = Arrays.asList(Color.values());
        Collections.shuffle(colors);
        Color[] colorList = new Color[size];
        colors.subList(0, size).toArray(colorList);

        for (int i = 0; i < size; i++) {
            Node node = new Node(i, size);
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
    public static void main(String[] args) {
        int graph_count = 4;
        Random random = new Random();

        List<Graph> graphs = new ArrayList<>();
        for (int i = 0; i < graph_count; i++) {
            Graph graph = new Graph(random.nextInt(9) + 2);
            graph.setRandom();
            graph.sort();
            graphs.add(graph);
        }
    }
}
