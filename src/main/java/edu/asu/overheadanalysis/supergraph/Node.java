package edu.asu.overheadanalysis.supergraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

public class Node {
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

    public void setRandom(ArrayList<Color> colorList) {
        Random random = new Random();
        boolean allZeros = true;
        for (int j = 0; j < pos; j++) {
            adj.put(colorList.get(j), random.nextInt(2));
            allZeros = adj.get(colorList.get(j)) == 0;
        }

        if (allZeros && pos > 0) {
            int j = random.nextInt(pos);
            adj.put(colorList.get(j), 1);
        }
    }
}
