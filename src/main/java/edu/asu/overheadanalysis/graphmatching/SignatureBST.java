package edu.asu.overheadanalysis.graphmatching;

class SignatureBST {
  SignatureNode root;

  SignatureBST() {
    this.root = null;
  }

  /**
   * Method to create an BST of signatures, the count on each node will give us how many of the
   * workloads have exactly the same signature.
   *
   * @param node New signature node to be added in the Tree.
   * @return BST of signatures of workload.
   */
  SignatureBST insertOrIncrement(SignatureNode node) {
    if (root == null) {
      this.root = node;
    } else {
      SignatureNode curr = this.root;
      while (curr != null) {
        if (node.signature.compareTo(curr.signature) > 0) {
          if (curr.right == null) {
            curr.right = node;
            curr = null;
          } else {
            curr = curr.right;
          }
        } else if (node.signature.compareTo(curr.signature) < 0) {
          if (curr.left == null) {
            curr.left = node;
            curr = null;
          } else {
            curr = curr.left;
          }
        } else {
          curr.incrementCount();
          curr = null;
        }
      }
    }
    return this;
  }
}
