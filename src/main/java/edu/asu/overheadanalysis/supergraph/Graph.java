package edu.asu.overheadanalysis.supergraph;

import java.util.*;

public class Graph {
    public ArrayList<Node> nodes;

    public Graph() {
        nodes = new ArrayList<Node>();
    }

    public int size() {
        return nodes.size();
    }

    public Node addNode(Color c) {
        Node node = new Node(nodes.size());
        node.setColor(c);
        nodes.add(node);

        return node;
    }

    public int getMapping(int i, int j) {
        return nodes.get(i).getMapping(nodes.get(j).color);
    }

    public static Graph mergeTwoGraphs(Graph a, Graph b) {
        int aCount = 0, bCount = 0;

        ArrayList<Node> nodes = new ArrayList<>();
        for (int i = 0; aCount < a.size() || bCount < b.size(); i++) {
            Node node = new Node(i);
            if (aCount < a.size() && bCount < b.size()) {

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

            } else if (aCount >= a.size()) {
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

        Graph newGraph = new Graph();
        newGraph.nodes = nodes;
        return newGraph;
    }

    public void setRandom(int size) {
        nodes = new ArrayList<>();
        List<Color> colors = Arrays.asList(Color.values());
        Collections.shuffle(colors);
        ArrayList<Color> colorList = new ArrayList<>(colors.subList(0, size));

        for (int i = 0; i < size; i++) {
            Node node = new Node(i);
            node.setRandom(colorList);
            node.setColor(colorList.get(i));
            nodes.add(node);
        }
    }

    public void print() {
        for (int i = 0; i < size(); i++) {
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
