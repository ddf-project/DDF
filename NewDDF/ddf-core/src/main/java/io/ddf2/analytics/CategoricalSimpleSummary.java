package io.ddf2.analytics;

import java.util.List;

/**
 * Created by echo on 2/5/16.
 */
public class CategoricalSimpleSummary extends SimpleSummary{
    private List<String> values;

    public void setValues(List<String> values) {
        this.values = values;
    }

    public List<String> getValues() {
        return this.values;
    }
}

