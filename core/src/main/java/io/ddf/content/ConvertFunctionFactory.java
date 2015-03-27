package io.ddf.content;


import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import io.ddf.DDF;
import org.jgrapht.EdgeFactory;

/**
 */
public class ConvertFunctionFactory implements EdgeFactory<Representation, ConvertFunction> {
  private DDF mDDF;
  private Table<Representation, Representation, ConvertFunction> mMap;

  public ConvertFunctionFactory(DDF ddf) {
    this.mDDF = ddf;
    this.mMap = HashBasedTable.create();
  }

  public void put(Representation startVertex, Representation endVertex, ConvertFunction convertFunction) {
    this.mMap.put(startVertex, endVertex, convertFunction);
  }

  public void remove(Representation startVertext, Representation endVertex) {
    this.mMap.remove(startVertext, endVertex);
  }

  @Override
  public ConvertFunction createEdge(Representation startVertex, Representation endVertex) {
    return this.mMap.get(startVertex, endVertex);
  }
}
