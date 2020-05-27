package edu.asu.overheadanalysis.graphmatching;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Random;

public class Main {
  /**
   * {{n}} = Total number of Trees. {{k}} = Average number of nodes in a tree.
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {
    int n = Integer.parseInt(args[0]);
    int k = Integer.parseInt(args[1]);

    Random rand = new Random();
    int lowerBound = 5;
    int upperBound = (k - lowerBound) + k;

    LinkedList<WorkLoadTree> lisOfTrees = new LinkedList<>();
    DataGen generator = new DataGen();
    int uniqueCount = (int) (0.4 * n);
    int duplicateCount = n - uniqueCount;

    // create a 40% unique trees.
    for (int i = 0; i < uniqueCount; i++) {
      lisOfTrees.add(generator.genRandomTree(rand.nextInt(upperBound) + lowerBound));
    }

    // create rest of 60% of the total trees,
    // such that they are identical to one of unique
    // trees.
    WorkLoadTree tempTree;
    for (int i = 0; i < duplicateCount; i++) {
      tempTree = lisOfTrees.get(rand.nextInt(uniqueCount));
      lisOfTrees.add(generator.getIdenticalTree(tempTree));
    }

    final long start0 = System.currentTimeMillis();

    // Get the signature for all the trees.
    LinkedList<String> signatureList = new LinkedList<>();
    for (WorkLoadTree tree : lisOfTrees) {
      signatureList.add(tree.getSignature());
    }

    final long end0 = System.currentTimeMillis();
    final long totalSignatureMilli = end0 - start0;

    final long start1 = System.currentTimeMillis();

    // Count the number of matching signatures.
    HashMap<String, Integer> signatureCount = new HashMap<>();
    for (String sign : signatureList) {
      if (signatureCount.containsKey(sign)) {
        signatureCount.put(sign, signatureCount.get(sign) + 1);
      } else {
        signatureCount.put(sign, 1);
      }
    }

    final long end1 = System.currentTimeMillis();
    final long totalCountMilli = end1 - start1;

    System.out.println("\nGraph Matching");
    System.out.println("\nSignature generation time: " + totalSignatureMilli + " milli seconds");
    System.out.println("\nMatching and taking counts time: " + totalCountMilli + " milli seconds");
  }
}
