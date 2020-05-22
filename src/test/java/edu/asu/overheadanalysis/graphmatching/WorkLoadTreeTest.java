package edu.asu.overheadanalysis.graphmatching;

import org.junit.Assert;
import org.junit.Test;

public class WorkLoadTreeTest {

  private WorkLoadNode r1 =
      new WorkLoadNode(5)
          .addChild(new WorkLoadNode(10).addChild(new WorkLoadNode(90)))
          .addChild(new WorkLoadNode(45).addChild(new WorkLoadNode(9)))
          .addChild(
              new WorkLoadNode(16).addChild(new WorkLoadNode(27)).addChild(new WorkLoadNode(32)));

  private WorkLoadNode r2 =
      new WorkLoadNode(40)
          .addChild(
              new WorkLoadNode(60)
                  .addChild(
                      new WorkLoadNode(78)
                          .addChild(new WorkLoadNode(54))
                          .addChild(new WorkLoadNode(4))))
          .addChild(
              new WorkLoadNode(80)
                  .addChild(
                      new WorkLoadNode(33)
                          .addChild(new WorkLoadNode(6))
                          .addChild(new WorkLoadNode(83))))
          .addChild(
              new WorkLoadNode(30)
                  .addChild(
                      new WorkLoadNode(76)
                          .addChild(new WorkLoadNode(12).addChild(new WorkLoadNode(17))))
                  .addChild(
                      new WorkLoadNode(61)
                          .addChild(new WorkLoadNode(46))
                          .addChild(new WorkLoadNode(93))));

  private WorkLoadTree t1 = new WorkLoadTree(r1);
  private WorkLoadTree t2 = new WorkLoadTree(r2);

  @Test
  public void testFirstTree() {
    String s1 = t1.getSignature(new WorkLoadNode.IntNodeComparator());
    Assert.assertEquals(s1, "51016459027329");
  }

  @Test
  public void testSecondTree() {
    String s2 = t2.getSignature(new WorkLoadNode.IntNodeComparator());
    System.out.println(s2);
    Assert.assertEquals(s2, "403060806176783346931245468317");
  }
}
