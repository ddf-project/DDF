package io.newddf;

import com.sun.istack.Nullable;
import io.ddf.analytics.IHandleAggregation;
import io.ddf.analytics.IHandleBinning;
import io.ddf.analytics.ISupportStatistics;
import io.ddf.content.*;
import io.ddf.etl.*;
import io.ddf.misc.IHandleMiscellany;
import io.ddf.misc.IHandleStreamingData;
import io.ddf.misc.IHandleTimeSeries;
import io.ddf.ml.ISupportML;
import io.ddf.ml.ISupportMLMetrics;
import io.newddf.datasource.IDataSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by sangdn on 12/28/15.
 */
public abstract class DDF implements IDDF {
    /*common properties*/
    protected IDataSource dataSource;
    /* common handler */
    protected ISupportStatistics mStatisticsSupporter;
    protected IHandleIndexing mIndexingHandler;
    protected IHandleJoins mJoinsHandler;
    protected IHandleMetaData mMetaDataHandler;
    protected IHandleMiscellany mMiscellanyHandler;
    protected IHandleMissingData mMissingDataHandler;
    protected IHandleMutability mMutabilityHandler;
    protected IHandleSql mSqlHandler;
    protected IHandlePersistence mPersistenceHandler;
    protected IHandleRepresentations mRepresentationHandler;
    protected IHandleReshaping mReshapingHandler;
    protected IHandleSchema mSchemaHandler;
    protected IHandleStreamingData mStreamingDataHandler;
    protected IHandleTimeSeries mTimeSeriesHandler;
    protected IHandleViews mViewHandler;
    protected ISupportML mMLSupporter;
    protected ISupportMLMetrics mMLMetricsSupporter;
    protected IHandleAggregation mAggregationHandler;
    protected IHandleBinning mBinningHandler;
    protected IHandleTransformations mTransformationHandler;

    public DDF(IDataSource ds) {
        this.dataSource = ds;
    }

    @Override
    public SqlResult sql(String query) {
        return null;
    }



    public static abstract class Builder<T extends DDF>{
        protected T ddf;
        protected Map<String,Object> mapProperties;
        public Builder(IDataSource dataSource){
            ddf = newInstance(dataSource);
            mapProperties = new HashMap<>();
        }
        protected  abstract T newInstance(IDataSource ds);
        public T build(){return ddf;}
        public Builder<T> setAggregationHandler(IHandleAggregation mAggregationHandler) {
            ddf.mAggregationHandler = mAggregationHandler; return this;
        }

        public Builder<T> setBinningHandler(IHandleBinning mBinningHandler) {
            ddf.mBinningHandler = mBinningHandler; return this;
        }

        public Builder<T> setIndexingHandler(IHandleIndexing mIndexingHandler) {
            ddf.mIndexingHandler = mIndexingHandler; return this;
        }

        public Builder<T> setJoinsHandler(IHandleJoins mJoinsHandler) {
            ddf.mJoinsHandler = mJoinsHandler; return this;
        }

        public Builder<T> setMetaDataHandler(IHandleMetaData mMetaDataHandler) {
            ddf.mMetaDataHandler = mMetaDataHandler; return this;
        }

        public Builder<T> setMiscellanyHandler(IHandleMiscellany mMiscellanyHandler) {
            ddf.mMiscellanyHandler = mMiscellanyHandler; return this;
        }

        public Builder<T> setMissingDataHandler(IHandleMissingData mMissingDataHandler) {
            ddf.mMissingDataHandler = mMissingDataHandler; return this;
        }

        public Builder<T> setMLMetricsSupporter(ISupportMLMetrics mMLMetricsSupporter) {
            ddf.mMLMetricsSupporter = mMLMetricsSupporter; return this;
        }

        public Builder<T> setMLSupporter(ISupportML mMLSupporter) {
            ddf.mMLSupporter = mMLSupporter; return this;
        }

        public Builder<T> setMutabilityHandler(IHandleMutability mMutabilityHandler) {
            ddf.mMutabilityHandler = mMutabilityHandler; return this;
        }

        public Builder<T> setPersistenceHandler(IHandlePersistence mPersistenceHandler) {
            ddf.mPersistenceHandler = mPersistenceHandler; return this;
        }

        public Builder<T> setRepresentationHandler(IHandleRepresentations mRepresentationHandler) {
            ddf.mRepresentationHandler = mRepresentationHandler; return this;
        }

        public Builder<T> setReshapingHandler(IHandleReshaping mReshapingHandler) {
            ddf.mReshapingHandler = mReshapingHandler; return this;
        }

        public Builder<T> setSchemaHandler(IHandleSchema mSchemaHandler) {
            ddf.mSchemaHandler = mSchemaHandler; return this;
        }

        public Builder<T> setSqlHandler(IHandleSql mSqlHandler) {
            ddf.mSqlHandler = mSqlHandler; return this;
        }

        public Builder<T> setStatisticsSupporter(ISupportStatistics mStatisticsSupporter) {
            ddf.mStatisticsSupporter = mStatisticsSupporter; return this;
        }

        public Builder<T> setStreamingDataHandler(IHandleStreamingData mStreamingDataHandler) {
            ddf.mStreamingDataHandler = mStreamingDataHandler; return this;
        }

        public Builder<T> setTimeSeriesHandler(IHandleTimeSeries mTimeSeriesHandler) {
            ddf.mTimeSeriesHandler = mTimeSeriesHandler; return this;
        }

        public Builder<T> setTransformationHandler(IHandleTransformations mTransformationHandler) {
            ddf.mTransformationHandler = mTransformationHandler; return this;
        }

        public Builder<T> setViewHandler(IHandleViews mViewHandler) {
            ddf.mViewHandler = mViewHandler; return this;
        }

        public Builder<T> putProperty(String key,Object value){
            mapProperties.put(key, value);
            return this;
        }
        public Builder<T> putProperty(Map<String,Object> mapProperties){
            this.mapProperties.putAll(mapProperties);
            return this;
        }
    }
}
