package io.ddf.ml;


import java.io.Serializable;
import java.util.Arrays;

public class RocMetric implements Serializable {

  public double[][] pred;
  public double auc;


  public RocMetric(double[][] _pred, double _auc) {
    this.pred = _pred;
    this.auc = _auc;
  }

  public void print() {
    int i = 0;
    while (i < this.pred.length) {
      if (this.pred[i] != null) {
        System.out.println(">>>\t" + i + "\t" + Arrays.toString(this.pred[i]));
      }
      i++;
    }
    System.out.println(">>>>> auc=" + auc);
  }

  public double computeAUC() {
    // filter null/NA in pred
    int i = 0;
    double previousTpr = 0;
    double previousFpr = 0;
    i = pred.length - 1;
    while (i >= 0) {
      // 0 index is for threshold value
      if (pred[i] != null) {
        // accumulate auc
        auc = auc + previousTpr * (pred[i][2] - previousFpr) + 0.5 * (pred[i][2] - previousFpr)
            * (pred[i][1] - previousTpr);
        // update previous
        previousTpr = pred[i][1];
        previousFpr = pred[i][2];
      }
      i = i - 1;
    }
    return (auc);
  }

  @Override
  public String toString() {
    return ("RocObject: pred = %.4f\t".format(Arrays.toString(this.pred[0])));
  }

  /**
   * Sum in-place to avoid new object alloc
   */
  public RocMetric addIn(RocMetric other) {
    // Sum tpr, fpr for each threshold
    int i = 0;
    // start from 1, 0-index is for threshold value
    int j = 1;
    while (i < this.pred.length) {
      if (this.pred[i] != null) {
        if (other.pred[i] != null) {
          j = 1;
          // P = P + P
          // N = N + N
          while (j < this.pred[i].length) {
            this.pred[i][j] = this.pred[i][j] + other.pred[i][j];
            j++;
          }
        }
      } else {
        if (other.pred[i] != null) {
          j = 0;
          // P = P + P
          // N = N + N
          // this.pred[i] is currently null so need to cretae new instance
          this.pred[i] = new double[3];
          while (j < other.pred[i].length) {
            this.pred[i][j] = other.pred[i][j];
            j = j + 1;
          }
        }
      }
      i = i + 1;
    }
    return (this);
  }
}
