package edu.asu.overheadanalysis.graphmatching;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

public class Main {
  /**
   * {{n}} = Total number of Trees. {{k}} = Number of identical Trees.
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {
    int n = Integer.parseInt(args[0]);
    int k = Integer.parseInt(args[1]);

    Random rand = new Random();

    LinkedList<WorkLoadTree> lisOfTrees = new LinkedList<WorkLoadTree>();
    DataGen generator = new DataGen();

    // create a query tree and k identical trees.
    WorkLoadTree query = generator.genRandomTree(rand.nextInt(96) + 4);
    lisOfTrees.add(query);
    for (int j = 0; j < k; j++) {
      lisOfTrees.add(generator.getIdenticalTree(query));
    }

    // create a random trees with total number being n,
    // including the previous k + 1 trees.
    for (int i = 0; i < n - k - 1; i++) {
      lisOfTrees.add(generator.genRandomTree(rand.nextInt(96) + 4));
    }

    HashMap<String, Integer> signatureCount = new HashMap<String, Integer>();
    for (WorkLoadTree tree : lisOfTrees) {
      String currSignature = tree.getSignature();
      if (signatureCount.containsKey(currSignature)) {
        signatureCount.put(currSignature, signatureCount.get(currSignature) + 1);
      } else {
        signatureCount.put(currSignature, 1);
      }
    }

    System.out.println(
        "Total number of identical "
            + "queries including the input :: "
            + signatureCount.get(query.getSignature()));
  }
}
