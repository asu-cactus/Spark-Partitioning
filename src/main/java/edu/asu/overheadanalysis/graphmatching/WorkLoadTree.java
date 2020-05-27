package edu.asu.overheadanalysis.graphmatching;

import java.util.*;

/** Tree to represent the workflow, each node in the tree is described by {{WorkLoadNode}}. */
class WorkLoadTree {
  WorkLoadNode root;

  WorkLoadTree(WorkLoadNode r) {
    this.root = r;
  }

  /**
   * Method to traverse the workload tree in BFT along with order. Along with BST, at each step
   * child nodes are sorted before being added to he queue.
   *
   * @return Ordered list of nodes in the tree.
   */
  private LinkedList<WorkLoadNode> orderedTraversal() {
    Comparator<WorkLoadNode> comp = new WorkNodeComparator();
    LinkedList<WorkLoadNode> res = new LinkedList<>();
    WorkLoadNode curr = this.root;
    ArrayList<WorkLoadNode> currChildren;
    Queue<WorkLoadNode> order = new LinkedList<>();
    order.add(curr);
    while (!order.isEmpty()) {
      curr = order.remove();
      res.add(curr);
      currChildren = curr.children;
      if (!currChildren.isEmpty()) {
        currChildren.sort(comp);
        order.addAll(currChildren);
      }
    }
    return res;
  }

  /**
   * Method to generate a signature from the workload tree. Isomorphic trees will have same
   * signature, which can be used for matching the two workloads.
   *
   * @return String representing the workload.
   */
  String getSignature() {
    StringBuilder strBuild = new StringBuilder();
    LinkedList<WorkLoadNode> seq = this.orderedTraversal();
    for (WorkLoadNode workLoadNode : seq) {
      strBuild.append(workLoadNode.data.toString());
    }
    return strBuild.toString();
  }

  private class WorkNodeComparator implements Comparator<WorkLoadNode> {

    public int compare(WorkLoadNode o1, WorkLoadNode o2) {
      return o1.data - o2.data;
    }

    public boolean equals(Object obj) {
      return false;
    }
  }
}
