package io.ddf2.handlers;

import io.ddf2.IDDF;

public interface IDDFHandler {
    /**
     * Every Handler have its associate ddf
     * @return Associated DDF
     */

    IDDF getDDF();
}
 
