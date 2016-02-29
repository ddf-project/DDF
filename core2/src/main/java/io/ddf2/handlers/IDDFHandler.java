package io.ddf2.handlers;

import io.ddf2.DDF;

public interface IDDFHandler<T extends DDF<T>> {
    /**
     * Every Handler have its associate ddf
     * @return Associated DDF
     */

    T getDDF();
}
 
