package org.apache.bookkeeper.bookie.utils;

import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;

public class InvalidStatsLogger implements StatsLogger {

    @Override
    public OpStatsLogger getOpStatsLogger(String name) {
        return null;
    }

    @Override
    public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
        return null;
    }

    @Override
    public Counter getCounter(String name) {
        return null;
    }

    @Override
    public Counter getThreadScopedCounter(String name) {
        return null;
    }

    @Override
    public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {

    }

    @Override
    public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {

    }

    @Override
    public StatsLogger scope(String name) {
        return null;
    }

    @Override
    public StatsLogger scopeLabel(String labelName, String labelValue) {
        return null;
    }

    @Override
    public void removeScope(String name, StatsLogger statsLogger) {

    }
}
