package edu.asu.overheadanalysis.graphmatching;

import java.util.ArrayList;
import java.util.Comparator;

class WorkLoadNode {
  Integer data;
  ArrayList<WorkLoadNode> children;

  WorkLoadNode(Integer data) {
    this.data = data;
    this.children = new ArrayList<WorkLoadNode>(0);
  }

  WorkLoadNode addChild(WorkLoadNode newChild) {
    this.children.add(newChild);
    return this;
  }

  static class IntNodeComparator implements Comparator<WorkLoadNode> {

    public int compare(WorkLoadNode o1, WorkLoadNode o2) {
      return o1.data - o2.data;
    }

    public boolean equals(Object obj) {
      return false;
    }
  }
}
