package io.ddf.ml;


import io.ddf.exception.DDFException;
import io.ddf.util.Utils.ClassMethod;

import java.lang.reflect.Method;

/**
 * Helper classes to identify/locate the train()/predict() methods in a given class or object
 */
public class MLClassMethods {

  public static final String DEFAULT_TRAIN_METHOD_NAME = "train";
  public static final String DEFAULT_PREDICT_METHOD_NAME = "predict";
  public static final Class<?>[] DEFAULT_PREDICT_METHOD_ARG_TYPES = new Class<?>[] { double[].class };


  /**
   *
   */
  public static class TrainMethod extends ClassMethod {

    public TrainMethod(String classHashMethodName, String trainMethodName, Object[] trainMethodArgs)
        throws DDFException {
      super(classHashMethodName, trainMethodName, trainMethodArgs);
    }

    /**
     * Override to search for methods that may contain initial arguments that precede our argTypes. These initial
     * arguments might be feature/target column specifications in a train() method. For example, we may be looking for a
     * method with the following signature:
     * <p/>
     * <code>
     * public SomeModel train(double[][] features, double[] outputs, otherArgs);
     * </code>
     * <p/>
     * but only the argTypes of otherArgs are specified here. Thus we do the argType matching from the end back to the
     * beginning.
     */
    @Override
    protected void findAndSetMethod(Class<?> theClass, String methodName, Class<?>... argTypes)
        throws NoSuchMethodException, SecurityException {

      if (argTypes == null) argTypes = new Class<?>[0];

      Method foundMethod = null;

      // Scan all methods
      for (Method method : theClass.getDeclaredMethods()) {
        if (!methodName.equalsIgnoreCase(method.getName())) continue;

        // Scan all method arg types, starting from the end, and see if they match with our supplied arg types
        Class<?>[] methodArgTypes = method.getParameterTypes();

        // Check that the number of args are correct:
        // the # of args in the method must be = 1 + the # of args supplied
        // ASSUMING that one of the args in the method is for input data

        if ((methodArgTypes.length - 1) == argTypes.length) {
          foundMethod = method;
          break;
        }
      }

      if (foundMethod != null) {
        foundMethod.setAccessible(true);
        this.setMethod(foundMethod);
      }
    }
  }



  /**
   *
   */
  public static class PredictMethod extends ClassMethod {

    public PredictMethod(Object model, String predictMethodName, Class<?>[] predictMethodArgTypes) throws DDFException {
      super(model, predictMethodName, predictMethodArgTypes);
    }

    public PredictMethod(Object model) throws DDFException {
      super(model, DEFAULT_PREDICT_METHOD_NAME, DEFAULT_PREDICT_METHOD_ARG_TYPES);
    }

  }
}
