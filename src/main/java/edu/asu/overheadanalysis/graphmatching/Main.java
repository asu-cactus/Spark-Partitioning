package edu.asu.overheadanalysis.graphmatching;

import java.util.*;
import java.util.concurrent.*;

public class Main {
  /**
   * {{n}} = Total number of Trees. {{k}} = Average number of nodes in a tree. {{lowerBound}} =
   * Lower bound for the number of nodes in a tree. {{threadCount}} = Number of threads {{bs}} =
   * Batch size
   *
   * @param args Command line arguments.
   */
  public static void main(String[] args) {
    int n = Integer.parseInt(args[0]);
    int k = Integer.parseInt(args[1]);
    int lowerBound = Integer.parseInt(args[2]);
    int threadCount = Integer.parseInt(args[3]);
    int bs = Integer.parseInt(args[4]);

    // Additional iteration to process the remainder.
    int numOfBatches = (n / bs) + 1;
    System.out.println("Total number of batches: " + numOfBatches);
    // Initially all the trees are yet to be processed
    // After each batch remaining count will keep decreasing.
    int treePoolRemaining = n;

    int upperBound = (k - lowerBound) + k;
    DataGen generator = new DataGen();
    Random rand = new Random();

    long totalSignatureMilli = 0;
    long totalCountMilli = 0;
    ConcurrentHashMap<String, Integer> signatureCount = new ConcurrentHashMap<>();

    for (int it = 0; it < numOfBatches; it++) {
      System.out.println();
      if (treePoolRemaining < bs) {
        bs = treePoolRemaining;
        treePoolRemaining = 0;
      } else if (treePoolRemaining == 0) {
        continue;
      } else {
        // For each batch {{bs}} number of trees are processed.
        treePoolRemaining -= bs;
      }
      System.out.println("Current batch ID: " + it);
      System.out.println("Number of Tress in current batch: " + bs);

      // If the total number of DAGs/Workload trees
      // is too low, use only one thread
      if (bs < 24) {
        threadCount = 1;
      }
      int uniqueCount = (int) (0.4 * bs);
      int duplicateCount = bs - uniqueCount;
      // create a 40% unique trees.
      ExecutorService uniqExec = Executors.newFixedThreadPool(threadCount);
      List<LinkedList<WorkLoadTree>> listsOfTrees =
          Collections.synchronizedList(new LinkedList<>());
      int threadUniqCount = uniqueCount / threadCount;
      int extraUnique = uniqueCount % threadCount;
      System.out.println("Generating unique trees");
      int uniqNumOfThreads = 0;
      for (int t = 0; t < threadCount; t++) {
        ++uniqNumOfThreads;
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
      System.out.println("Unique trees threads: " + uniqNumOfThreads);
      // wait for unique trees completion.
      uniqExec.shutdown();
      try {
        uniqExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException ie) {
        System.out.println("Error in generating unique trees");
        ie.printStackTrace();
      }
      int numOfUniqSets = listsOfTrees.size();
      System.out.println("Number of lists of unique DAGs: " + listsOfTrees.size());

      // create rest of 60% of the total trees,
      // such that they are identical to one of the
      // unique trees.
      System.out.println("Generating duplicate trees");
      ExecutorService dupExec = Executors.newFixedThreadPool(threadCount);
      int threadDupCount = duplicateCount / threadCount;
      int extraDup = duplicateCount % threadCount;
      int dupNumOfThreads = 0;
      for (int t = 0; t < threadCount; t++) {
        ++dupNumOfThreads;
        if (t == 0) {
          dupExec.submit(
              () ->
                  genDupTrees(
                      threadDupCount + extraDup, numOfUniqSets, generator, rand, listsOfTrees));
        } else {
          dupExec.submit(
              () -> genDupTrees(threadDupCount, numOfUniqSets, generator, rand, listsOfTrees));
        }
      }
      System.out.println("Duplicate trees threads: " + dupNumOfThreads);
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
      List<LinkedList<String>> signatures = Collections.synchronizedList(new LinkedList<>());
      int signNumOfThreads = 0;
      for (LinkedList<WorkLoadTree> treeSets : listsOfTrees) {
        ++signNumOfThreads;
        signExec.submit(() -> genSignatures(treeSets, signatures));
      }
      System.out.println("Signature generation threads: " + signNumOfThreads);
      // wait for signature generation to complete.
      signExec.shutdown();
      try {
        signExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException ie) {
        System.out.println("Error in generating signatures");
        ie.printStackTrace();
      }
      final long end0 = System.currentTimeMillis();
      totalSignatureMilli += end0 - start0;

      // Count the number of matching signatures.
      final long start1 = System.currentTimeMillis();
      System.out.println("Matching Started");
      ExecutorService matchExec = Executors.newFixedThreadPool(threadCount);
      for (LinkedList<String> signs : signatures) {
        matchExec.submit(() -> signMatching(signs, signatureCount));
      }
      // waiting for all the matching threads to finish.
      matchExec.shutdown();
      try {
        matchExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
      } catch (InterruptedException ie) {
        System.out.println("Error in signature matching and counting");
        ie.printStackTrace();
      }
      final long end1 = System.currentTimeMillis();
      totalCountMilli += end1 - start1;
    } // End of For-loop for each batch

    System.out.println("\nGraph Matching results");
    System.out.println("Signature generation time: " + totalSignatureMilli + " milliseconds");
    System.out.println("Matching and taking counts time: " + totalCountMilli + " milliseconds");
  } //  End of the main function

  /**
   * Method to generate unique DAGs/Trees
   *
   * @param num Number of unique trees to generate
   * @param upper Upper bound on number of nodes in a tree
   * @param lower Lower bound on the number of nodes in a tree
   * @param generator Data Generator class instance
   * @param rand Random number generator instance
   * @param treeSets Sets of unique trees
   */
  private static void genUniqueTrees(
      int num,
      int upper,
      int lower,
      DataGen generator,
      Random rand,
      List<LinkedList<WorkLoadTree>> treeSets) {
    LinkedList<WorkLoadTree> treeList = new LinkedList<>();
    for (int i = 0; i < num; i++) {
      treeList.add(generator.genRandomTree(rand.nextInt(upper - lower) + lower));
    }
    treeSets.add(treeList);
  }

  /**
   * Method to get the duplicate references
   *
   * @param num Number of duplicate references to generate
   * @param numOfUniqList Number of sets of unique trees
   * @param generator Data Generator class instance
   * @param rand Random number generator instance
   * @param treeSets Sets of trees to process
   */
  private static void genDupTrees(
      int num,
      int numOfUniqList,
      DataGen generator,
      Random rand,
      List<LinkedList<WorkLoadTree>> treeSets) {
    WorkLoadTree original = treeSets.get(rand.nextInt(numOfUniqList)).get(rand.nextInt(10));
    LinkedList<WorkLoadTree> treeList = new LinkedList<>();
    for (int i = 0; i < num; i++) {
      treeList.add(generator.getIdenticalTree(original));
    }
    treeSets.add(treeList);
  }

  /**
   * Method to generate signatures for the given list of trees
   *
   * @param treesToProcess List of trees for which signature is to be generated
   * @param signatures Sets of signatures for all the trees
   */
  private static void genSignatures(
      LinkedList<WorkLoadTree> treesToProcess, List<LinkedList<String>> signatures) {
    LinkedList<String> signList = new LinkedList<>();
    for (WorkLoadTree tree : treesToProcess) {
      signList.add(tree.getSignature());
    }
    signatures.add(signList);
  }

  /**
   * Method for matching the signatures and counting duplicates
   *
   * @param signatures Sets of signatures for all the trees
   * @param signCountMap Hash Map of signature to count
   */
  private static void signMatching(
      LinkedList<String> signatures, ConcurrentHashMap<String, Integer> signCountMap) {
    for (String sign : signatures) {
      if (signCountMap.containsKey(sign)) {
        signCountMap.put(sign, signCountMap.get(sign) + 1);
      } else {
        signCountMap.put(sign, 1);
      }
    }
  }
} // End of the Main class
