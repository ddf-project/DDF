//package io.ddf2.bigquery.handlers;
//
//import io.ddf2.DDF;
//import io.ddf2.DDFException;
//import io.ddf2.IDDF;
//import io.ddf2.analytics.HistogramBin;
//import io.ddf2.handlers.IBinningHandler;
//
//import java.util.List;
//
//public class BinningHandler implements io.ddf2.handlers.IBinningHandler {
//
//
//    /**
//     * Compute factor of columns using histogram.
//     *
//     * @param column        The column name.
//     * @param binningType   Currently support: "EQUALINTERVAL" "EQUAlFREQ" "custom" // TODO: change to enum
//     * @param numBins       Num of factors. For factors, set numBins to 0.
//     * @param breaks        The list of break points, for example [2, 4, 6, 8]. Set this param to null if binningType is not
//     *                      custom.
//     * @param includeLowest Whether to include the lowest value.
//     * @param right         Whether to include the right value.
//     * @return
//     * @throws DDFException
//     */
//    @Override
//    public IDDF binning(String column, String binningType, int numBins, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
//        return null;
//    }
//
//    @Override
//    public IDDF binningCustom(String column, double[] breaks, boolean includeLowest, boolean right) throws DDFException {
//        return null;
//    }
//
//    @Override
//    public IDDF binningEq(String column, int numBin, boolean includeLowest, boolean right) throws DDFException {
//        return null;
//    }
//
//    @Override
//    public IDDF binningEqFreq(String column, int numBin, boolean includeLowest, boolean right) throws DDFException {
//        return null;
//    }
//
//    @Override
//    public List<HistogramBin> getHistogram(String column, int numBin) throws DDFException {
//        return null;
//    }
//
//    /**
//     * Every Handler have its associate ddf
//     *
//     * @return Associated DDF
//     */
//    @Override
//    public IDDF getDDF() {
//        return null;
//    }
//}
//
