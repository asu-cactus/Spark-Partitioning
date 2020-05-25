package edu.asu.overheadanalysis.supergraph;

import java.util.*;

public class Graph {
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
