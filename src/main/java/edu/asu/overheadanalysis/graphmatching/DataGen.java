package edu.asu.overheadanalysis.graphmatching;

import java.util.*;

class DataGen {
  private Random randGen = new Random();

  /**
   * Method to random generate a tree with total number of nodes given by the parameter
   * {{numOfNodes}}. 1. Randomly generate WorkLoad nodes with random work assignment from 1 to 100.
   * 2. For each node randomly decide the number of child nodes. 3. Add the child nodes to the
   * queue, which maintains the order in which random child nodes will be assigned. It is breadth
   * first traversal.
   *
   * @param numOfNodes Number of nodes in the initial tree.
   * @return Random WorkLoadTree object.
   */
  public WorkLoadTree genRandomTree(int numOfNodes) {
    ArrayList<WorkLoadNode> listOfNodes = new ArrayList<WorkLoadNode>(numOfNodes);
    for (int i = 0; i < numOfNodes; i++) {
      listOfNodes.add(i, new WorkLoadNode(randGen.nextInt(100) + 1));
    }
    WorkLoadNode root = listOfNodes.remove(0);
    WorkLoadNode curr = root;
    Queue<WorkLoadNode> genQueue = new LinkedList<WorkLoadNode>();
    while (!listOfNodes.isEmpty()) {
      int numOfChildren = randGen.nextInt(6) + 1;
      int nodeLen = listOfNodes.size();
      if (numOfChildren > nodeLen) {
        numOfChildren = nodeLen;
      }
      ArrayList<WorkLoadNode> randChildNodes = new ArrayList<WorkLoadNode>(numOfChildren);
      for (int i = 0; i < numOfChildren; i++) {
        genQueue.add(listOfNodes.get(0));
        randChildNodes.add(i, listOfNodes.remove(0));
      }
      curr.children = randChildNodes;
      curr = genQueue.remove();
    }

    return new WorkLoadTree(root);
  }

  /**
   * Method to get the identical tree from the given input tree by shuffling the nodes at each
   * level.
   *
   * <p>This method traverse the tree in BST fashion, shuffles the child nodes to reset the order.
   *
   * @param tree Input Tree
   * @return Shuffled identical tree.
   */
  public WorkLoadTree getIdenticalTree(WorkLoadTree tree) {
    WorkLoadNode curr = tree.root;
    ArrayList<WorkLoadNode> currChildren;
    Queue<WorkLoadNode> queue = new LinkedList<WorkLoadNode>();
    queue.add(curr);
    while (!queue.isEmpty()) {
      curr = queue.remove();
      currChildren = curr.children;
      if (!currChildren.isEmpty()) {
        Collections.shuffle(currChildren);
        // reset the child nodes after shuffle.
        curr.children = currChildren;
        queue.addAll(currChildren);
      }
    }
    return tree;
  }
}
