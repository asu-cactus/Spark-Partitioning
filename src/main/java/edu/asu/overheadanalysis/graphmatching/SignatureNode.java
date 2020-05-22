package edu.asu.overheadanalysis.graphmatching;

class SignatureNode {
  String signature;
  Integer count;
  SignatureNode left;
  SignatureNode right;

  SignatureNode(String sign) {
    this.signature = sign;
    this.count = 1;
    this.left = null;
    this.right = null;
  }

  void incrementCount(Integer val) {
    this.count += val;
  }

  void incrementCount() {
    this.count += 1;
  }
}
