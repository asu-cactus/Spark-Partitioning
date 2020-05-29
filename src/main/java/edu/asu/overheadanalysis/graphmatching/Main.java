package edu.asu.overheadanalysis.graphmatching;

import java.util.*;
import java.util.concurrent.*;

public class Main {
  /**
   * {{n}} = Total number of Trees. {{k}} = Average number of nodes in a tree. {{lowerBound}} =
   * Lower bound for the number of nodes in a tree. {{threadCount}} = Number of threads
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {
    int n = Integer.parseInt(args[0]);
    int k = Integer.parseInt(args[1]);
    int lowerBound = Integer.parseInt(args[2]);
    int threadCount = Integer.parseInt(args[3]);

    // If the total number of DAGs/Workload trees
    // is too low, use only one thread
    if (n < 24) {
      threadCount = 1;
    }

    int upperBound = (k - lowerBound) + k;
    int uniqueCount = (int) (0.4 * n);
    int duplicateCount = n - uniqueCount;
    DataGen generator = new DataGen();
    Random rand = new Random();

    // create a 40% unique trees.
    ExecutorService uniqExec = Executors.newFixedThreadPool(threadCount);
    List<LinkedList<WorkLoadTree>> listsOfTrees = Collections.synchronizedList(new LinkedList<>());
    int threadUniqCount = uniqueCount / threadCount;
    int extraUnique = uniqueCount % threadCount;
    System.out.println("Generating unique trees");
    for (int t = 0; t < threadCount; t++) {
      if (t == 0) {
        uniqExec.submit(
            () ->
                genUniqueTrees(
                    threadUniqCount + extraUnique,
                    upperBound,
                    lowerBound,
                    generator,
                    rand,
                    listsOfTrees));
      } else {
        uniqExec.submit(
            () ->
                genUniqueTrees(
                    threadUniqCount, upperBound, lowerBound, generator, rand, listsOfTrees));
      }
    }
    // wait for unique trees completion.
    uniqExec.shutdown();
    try {
      uniqExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      System.out.println("Error in generating unique trees");
      ie.printStackTrace();
    }
    int numOfUniqSets = listsOfTrees.size();
    System.out.println("Number of sets of unique DAGs: " + listsOfTrees.size());

    // create rest of 60% of the total trees,
    // such that they are identical to one of the
    // unique trees.
    System.out.println("Generating duplicate trees");
    ExecutorService dupExec = Executors.newFixedThreadPool(threadCount);
    int threadDupCount = duplicateCount / threadCount;
    int extraDup = duplicateCount % threadCount;
    for (int t = 0; t < threadCount; t++) {
      if (t == 0) {
        dupExec.submit(
            () ->
                genDupTrees(
                    threadDupCount + extraDup,
                    numOfUniqSets,
                    threadUniqCount,
                    generator,
                    rand,
                    listsOfTrees));
      } else {
        dupExec.submit(
            () ->
                genDupTrees(
                    threadDupCount, numOfUniqSets, threadUniqCount, generator, rand, listsOfTrees));
      }
    }
    // wait for duplicate trees completion.
    dupExec.shutdown();
    try {
      dupExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      System.out.println("Error in generating duplicate trees");
      ie.printStackTrace();
    }

    // Get the signature for all the trees.
    final long start0 = System.currentTimeMillis();
    System.out.println("Signature generation started");
    ExecutorService signExec = Executors.newFixedThreadPool(threadCount);
    LinkedList<Future<LinkedList<String>>> futures = new LinkedList<>();
    LinkedList<String> signatures = new LinkedList<>();
    for (LinkedList<WorkLoadTree> treeSets : listsOfTrees) {
      futures.add(signExec.submit(() -> genSignatures(treeSets)));
    }
    for (Future<LinkedList<String>> f : futures) {
      try {
        signatures.addAll(f.get());
      } catch (InterruptedException | ExecutionException ie) {
        System.out.println("Error in generating signatures");
        ie.printStackTrace();
      }
    }
    final long end0 = System.currentTimeMillis();
    final long totalSignatureMilli = end0 - start0;

    // Count the number of matching signatures.
    final long start1 = System.currentTimeMillis();
    System.out.println("Matching Started");
    HashMap<String, Integer> signatureCount = new HashMap<>();

    for (String sign : signatures) {
      if (signatureCount.containsKey(sign)) {
        signatureCount.put(sign, signatureCount.get(sign) + 1);
      } else {
        signatureCount.put(sign, 1);
      }
    }
    final long end1 = System.currentTimeMillis();
    final long totalCountMilli = end1 - start1;

    System.out.println("\nGraph Matching results");
    System.out.println("Signature generation time: " + totalSignatureMilli + " milliseconds");
    System.out.println("Matching and taking counts time: " + totalCountMilli + " milliseconds");
  } //  End of the main function

  private static void genUniqueTrees(
      int num,
      int upper,
      int lower,
      DataGen generator,
      Random rand,
      List<LinkedList<WorkLoadTree>> treeSets) {
    System.out.println("Unique Thread");
    LinkedList<WorkLoadTree> treeList = new LinkedList<>();
    for (int i = 0; i < num; i++) {
      treeList.add(generator.genRandomTree(rand.nextInt(upper - lower) + lower));
    }
    treeSets.add(treeList);
  }

  private static void genDupTrees(
      int num,
      int numOfUniqList,
      int perListUniqCount,
      DataGen generator,
      Random rand,
      List<LinkedList<WorkLoadTree>> treeSets) {
    System.out.println("Duplicate Thread");
    WorkLoadTree original =
        treeSets.get(rand.nextInt(numOfUniqList)).get(rand.nextInt(perListUniqCount));
    LinkedList<WorkLoadTree> treeList = new LinkedList<>();
    for (int i = 0; i < num; i++) {
      treeList.add(generator.getIdenticalTree(original));
    }
    treeSets.add(treeList);
  }

  private static LinkedList<String> genSignatures(LinkedList<WorkLoadTree> treesToProcess) {
    System.out.println("Signature Thread");
    LinkedList<String> signList = new LinkedList<>();
    for (WorkLoadTree tree : treesToProcess) {
      signList.add(tree.getSignature());
    }
    return signList;
  }
} // End of the Main class
