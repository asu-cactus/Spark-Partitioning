package edu.asu.overheadanalysis.graphmatching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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

    // create a 40% unique trees.
    ExecutorService uniqExec = Executors.newFixedThreadPool(threadCount);
    List<ArrayList<WorkLoadTree>> rawSetsOfTrees = Collections.synchronizedList(new ArrayList<>());
    int threadUniqCount = uniqueCount / threadCount;
    int extraUnique = uniqueCount % threadCount;
    System.out.println("Generating unique trees");
    for (int t = 0; t < threadCount; t++) {
      if (t == 0) {
        uniqExec.submit(
            new UniqueTrees(threadUniqCount + extraUnique, rawSetsOfTrees, upperBound, lowerBound));
      } else {
        uniqExec.submit(new UniqueTrees(threadUniqCount, rawSetsOfTrees, upperBound, lowerBound));
      }
    }
    // wait for unique trees completion.
    uniqExec.shutdown();
    try {
      uniqExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
    System.out.println("Number of sets of unique DAGs: " + rawSetsOfTrees.size());

    // create rest of 60% of the total trees,
    // such that they are identical to one of the
    // unique trees.
    System.out.println("Generating duplicate trees");
    ExecutorService dupExec = Executors.newFixedThreadPool(threadCount);
    int threadDupCount = duplicateCount / threadCount;
    int extraDup = duplicateCount % threadCount;
    WorkLoadTree tempTree;
    for (int t = 0; t < threadCount; t++) {
      tempTree = rawSetsOfTrees.get(t).get(0);
      if (t == 0) {
        dupExec.submit(new DuplicateTrees(threadDupCount + extraDup, rawSetsOfTrees, tempTree));
      } else {
        dupExec.submit(new DuplicateTrees(threadDupCount, rawSetsOfTrees, tempTree));
      }
    }
    // wait for duplicate trees completion.
    dupExec.shutdown();
    try {
      dupExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }

    // Get the signature for all the trees.
    final long start0 = System.currentTimeMillis();
    System.out.println("Signature generation started");
    ExecutorService signExec = Executors.newFixedThreadPool(threadCount);
    List<ArrayList<String>> signatureSets = Collections.synchronizedList(new ArrayList<>());
    for (ArrayList<WorkLoadTree> treeSets : rawSetsOfTrees) {
      signExec.submit(new GetSignatures(treeSets, signatureSets));
    }
    // wait for signature generation to complete.
    signExec.shutdown();
    try {
      signExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
    final long end0 = System.currentTimeMillis();
    final long totalSignatureMilli = end0 - start0;

    // Count the number of matching signatures.
    final long start1 = System.currentTimeMillis();
    System.out.println("Matching Started");
    ExecutorService matchExec = Executors.newFixedThreadPool(threadCount);

    ConcurrentHashMap<String, Integer> signatureCount = new ConcurrentHashMap<>();

    for (ArrayList<String> signsSet : signatureSets) {
      matchExec.submit(new MatchingSignatures(signsSet, signatureCount));
    }
    matchExec.shutdown();
    try {
      matchExec.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    } catch (InterruptedException ie) {
      ie.printStackTrace();
    }
    final long end1 = System.currentTimeMillis();
    final long totalCountMilli = end1 - start1;

    System.out.println("\nGraph Matching results");
    System.out.println("Signature generation time: " + totalSignatureMilli + " milli seconds");
    System.out.println("Matching and taking counts time: " + totalCountMilli + " milli seconds");
  }

  /** Runnable class to create unique threes. */
  static class UniqueTrees implements Runnable {

    private final int num;
    private int upper;
    private int lower;
    private DataGen generator = new DataGen();
    private Random rand = new Random();
    private List<ArrayList<WorkLoadTree>> treeSets;

    public UniqueTrees(int n, List<ArrayList<WorkLoadTree>> trees, int u, int l) {
      this.num = n;
      this.upper = u;
      this.lower = l;
      this.treeSets = trees;
    }

    @Override
    public void run() {
      ArrayList<WorkLoadTree> treeList = new ArrayList<>();
      try {
        System.out.println("Unique Thread");
        for (int i = 0; i < num; i++) {
          treeList.add(generator.genRandomTree(rand.nextInt(upper - lower) + lower));
        }
        treeSets.add(treeList);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  /** Runnable class to create duplicate trees, from the given original tree. */
  static class DuplicateTrees implements Runnable {

    private final int num;
    private WorkLoadTree original;
    private DataGen generator = new DataGen();
    private List<ArrayList<WorkLoadTree>> treeSets;

    public DuplicateTrees(int n, List<ArrayList<WorkLoadTree>> trees, WorkLoadTree o) {
      this.num = n;
      this.treeSets = trees;
      this.original = o;
    }

    @Override
    public void run() {
      try {
        System.out.println("Duplicate Thread");
        ArrayList<WorkLoadTree> treeList = new ArrayList<>();
        for (int i = 0; i < num; i++) {
          treeList.add(generator.getIdenticalTree(this.original));
        }
        treeSets.add(treeList);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  /** Runnable class to generate signatures for the given sets fo trees. */
  static class GetSignatures implements Runnable {

    private ArrayList<WorkLoadTree> treesToProcess;
    private List<ArrayList<String>> signSets;

    public GetSignatures(ArrayList<WorkLoadTree> t, List<ArrayList<String>> s) {
      this.treesToProcess = t;
      this.signSets = s;
    }

    @Override
    public void run() {
      try {
        System.out.println("Signature Generation Thread");
        ArrayList<String> signList = new ArrayList<>();
        for (WorkLoadTree tree : treesToProcess) {
          signList.add(tree.getSignature());
        }
        signSets.add(signList);
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }

  /** Runnable class for matching the given signature and counting the duplicates. */
  static class MatchingSignatures implements Runnable {

    private ArrayList<String> signToProcess;
    private ConcurrentHashMap<String, Integer> countMap;

    public MatchingSignatures(ArrayList<String> s, ConcurrentHashMap<String, Integer> map) {
      this.signToProcess = s;
      this.countMap = map;
    }

    @Override
    public void run() {
      try {
        System.out.println("Matching Thread");
        for (String sign : signToProcess) {
          if (countMap.containsKey(sign)) {
            countMap.put(sign, countMap.get(sign) + 1);
          } else {
            countMap.put(sign, 1);
          }
        }
      } catch (Exception ex) {
        ex.printStackTrace();
      }
    }
  }
}
