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
    ArrayList<Integer> adj;
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
        adj = new ArrayList<>(Arrays.asList(new Integer[n]));
        Collections.fill(adj, 0);
    }

    public void print() {
        System.out.print(color.toString() + ": ");
        for (int i = 0; i < size; i++)
            System.out.print(adj.get(i) + ", ");
        System.out.print("\n");
    }

    public void setRandom() {
        Random random = new Random();
        boolean allZeros = true;
        for (int j = 0; j < pos; j++) {
            adj.set(j, random.nextInt(2));
            allZeros = adj.get(j) == 0;
        }

        if (allZeros && pos > 0) {
            int j = random.nextInt(pos);
            adj.set(j, 1);
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
            node.setRandom();
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
//    public static Color[] assignColors(int m) {
//        List<Color> colors = Arrays.asList(Color.values());
//        Collections.shuffle(colors);
//        Color[] colorList = new Color[m];
//        return colors.subList(0, m).toArray(colorList);
//    }
//
//    public static Integer[][] randomMatrix(int m, int n) {
//        Random random = new Random();
//        Integer[][] matrix = new Integer[m][n];
//        for (int i = 0; i < m; i++) {
//            boolean allZeros = true;
//            for (int j = 0; j < n; j++) {
//                matrix[i][j] = i == j ? 0 : random.nextInt(2);
//                allZeros = matrix[i][j] == 0;
//            }
//
//            if (allZeros) {
//                int pos = random.nextInt(n);
//                if (pos == i) {
//                    pos = (pos + 1) % n;
//                }
//                matrix[i][pos] = 1;
//            }
//        }
//
//        return matrix;
//    }
//
//    public static void printMatrix(Integer[][] matrix) {
//        for (Integer[] integers : matrix) {
//            for (int j = 0; j < matrix[0].length; j++) {
//                System.out.print(integers[j] + ", ");
//            }
//            System.out.print("\n");
//        }
//    }
//
//    public static void printColors(Color[] colors) {
//        for (int i = 0; i < colors.length; i++) {
//            System.out.print(colors[i].toString() + ", ");
//        }
//        System.out.print("\n");
//    }

    public static void main(String[] args) {
        int nodes = 4;
        Graph graph = new Graph(nodes);
        graph.setRandom();
        graph.print();

        graph.sort();
        System.out.println("---------------------------------------------------");

        graph.print();

//        int nodes = 4;
//        Integer[][] matrix = randomMatrix(nodes, nodes);
//        Color[] colors = assignColors(nodes);
//
//        printMatrix(matrix);
//        printColors(colors);
//
//        ArrayList<Integer> ids = new ArrayList<Integer>();
//        for (int i = 0; i < nodes; i++)
//            ids.add(i);
//        ids.sort(new Comparator<Integer>(){
//            public int compare(Integer i, Integer j) {
//                return colors[i].compareTo(colors[j]);
//            }
//        });
//
//        for (int i = 0; i < nodes; i++) {
//            Color temp = colors[i];
//            colors[i] = colors[ids.get(i)];
//            colors[ids.get(i)] = temp;
//        }
//
//        for (int i = 0; i < nodes; i++) {
//            Integer[] temp = matrix[i];
//            matrix[i] = matrix[ids.get(i)];
//            matrix[ids.get(i)] = temp;
//        }
//
//        for (int i = 0; i < nodes; i++)
//            System.out.print(ids.get(i) + ", ");
//        System.out.print("\n");
//
//        printMatrix(matrix);
//        printColors(colors);
    }
}
