package edu.asu.overheadanalysis.graphmatching;

import java.util.*;

/** Tree to represent the workflow, Each node in the tree is described by {{WorkLoadNode}}. */
class WorkLoadTree {
  WorkLoadNode root;

  WorkLoadTree(WorkLoadNode r) {
    this.root = r;
  }

  /**
   * Method to traverse the workload tree in BFT along with order. Along with BST, at each step
   * child nodes are sorted before being added to he queue.
   *
   * @param comp Comparator for the nodes being sorted.
   * @return Ordered list of nodes in the tree.
   */
  private LinkedList<WorkLoadNode> orderedTraversal(Comparator<WorkLoadNode> comp) {
    LinkedList<WorkLoadNode> res = new LinkedList<WorkLoadNode>();
    WorkLoadNode curr = this.root;
    ArrayList<WorkLoadNode> currChildren;
    Queue<WorkLoadNode> order = new LinkedList<WorkLoadNode>();
    order.add(curr);
    while (!order.isEmpty()) {
      curr = order.remove();
      res.add(curr);
      currChildren = curr.children;
      if (!currChildren.isEmpty()) {
        Collections.sort(currChildren, comp);
        order.addAll(currChildren);
      }
    }
    return res;
  }

  /**
   * Method to generate a signature from the workload tree. Isomorphic trees will have same
   * signature, which can be used for matching the two workloads.
   *
   * @param comp Comparator for the nodes, to be sorted in order to create a signature.
   * @return String representing the workload.
   */
  String getSignature(Comparator<WorkLoadNode> comp) {
    StringBuilder strBuild = new StringBuilder();
    LinkedList<WorkLoadNode> seq = this.orderedTraversal(comp);
    for (WorkLoadNode workLoadNode : seq) {
      strBuild.append(workLoadNode.data.toString());
    }
    return strBuild.toString();
  }
}
