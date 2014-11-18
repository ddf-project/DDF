package io.ddf.content;


import io.ddf.DDF;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DirectedWeightedMultigraph;

/**
 */
public class RepresentationsGraph {

  private DDF mDDF;
  private DirectedWeightedMultigraph<Representation, ConvertFunction> mGraph;
  private ConvertFunctionFactory mConvertFunctionFactory;

  public RepresentationsGraph(DDF ddf) {
    this.mDDF = ddf;
    this.mConvertFunctionFactory = new ConvertFunctionFactory(this.mDDF);
    this.mGraph = new DirectedWeightedMultigraph<Representation, ConvertFunction>(this.mConvertFunctionFactory);
  }

  public ConvertFunction addEdge(Representation startVertex, Representation endVertex,
      ConvertFunction convertFunction) {
    this.mConvertFunctionFactory.put(startVertex, endVertex, convertFunction);
    this.mGraph.addVertex(startVertex);
    this.mGraph.addVertex(endVertex);
    return this.mGraph.addEdge(startVertex, endVertex);
  }

  public GraphPath<Representation, ConvertFunction> getShortestPath(Representation startVertex,
      Representation endVertex) {
    try {
      return new DijkstraShortestPath<Representation, ConvertFunction>(this.mGraph, startVertex, endVertex).getPath();
    } catch (Exception e) {
      return null;
    }
  }
}
