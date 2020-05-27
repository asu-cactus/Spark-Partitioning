package edu.asu.overheadanalysis.graphmatching;

import java.util.ArrayList;

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
}
