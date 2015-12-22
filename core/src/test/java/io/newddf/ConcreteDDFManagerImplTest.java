package io.newddf;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * Created by sangdn on 12/21/15.
 */
public class ConcreteDDFManagerImplTest {

    @Test
    public void testNewConcreteDDFManagerImpl(){
        ConcreteDDFManagerImpl concreteDDFManager = NewDDFManager.newInstance(ConcreteDDFManagerImpl.class, Collections.emptyMap());
        assert concreteDDFManager != null;
    }

}