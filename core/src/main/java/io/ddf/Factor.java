package io.ddf;


import io.ddf.exception.DDFException;

import java.io.Serializable;
import java.util.*;

/**
 * A factor is a column vector representing a categorical variable. It is presented as a coding of numeric values, which
 * map to a set of levels of character (string) names. For example, we may have the following categorical variable:
 * <p/>
 * <p/>
 * <pre>
 *   [ Sunday, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday ]
 * </pre>
 * <p/>
 * which we choose to represent internally as
 * <p/>
 * <pre>
 *   [ 0, 1, 2, 3, 4, 5, 6 ]
 * </pre>
 * <p/>
 * In this case, we say this factor has 7 levels, with the mapping of Sunday=0, Monday=1, etc. References:
 * <ul>
 * <li>https://www.inkling.com/read/r-cookbook-paul-teetor-1st/chapter-5/recipe-5-4</li>
 * <li>http://www.ats.ucla.edu/stat/r/modules/factor_variables.htm</li>
 * <li>https://www.stat.berkeley.edu/classes/s133/factors.html</li>
 * </ul>
 * <p/>
 * Now, when doing regression against a categorical variable, we most likely would not want to use those numeric values
 * directly. The reason is Tuesday=2 should not be interpreted as "twice the value of" Monday=1. So, before performing
 * regression, we want to map this factor column into multiple "dummy" columns, and perform regression against these
 * columns instead. Hence the term "dummy coding".
 * <p/>
 * When dummy-coding a factor, there is one reference level, and the other levels are "referenced" against it. Thus, an
 * L-level factor will expand to L-1 numeric vectors/columns. This expansion is done via a mapping, using an [L x L-1]
 * matrix. Specifically, each level will map to L-1 values. One example such matrix might look like this, for L=4:
 * <p/>
 * <pre>
 *   | 0 0 0 |
 *   | 1 0 0 |
 *   | 0 1 0 |
 *   | 0 0 1 |
 * </pre>
 * <p/>
 * The above is also known as "Treatment" coding, i.e., "received treatment" or not. For another example, consider the
 * "Simple" coding, which contrasts each level with a fixed reference level, normally taken to be the mean of all the
 * levels.
 * <p/>
 * <pre>
 *   | -0.25 -0.25 -0.25 |
 *   |  0.75 -0.25 -0.25 |
 *   | -0.25  0.75 -0.25 |
 *   | -0.25 -0.25  0.75 |
 * </pre>
 * <p/>
 * For a comprehensive list of coding schemes, see http://statsmodels.sourceforge.net/devel/contrasts.html.
 * <p/>
 * <p/>
 * References: Dummy Coding (for Regression with Categorical Variables)
 * <ul>
 * <li>http://statsmodels.sourceforge.net/devel/contrasts.html</li>
 * <li>http://www.ats.ucla.edu/stat/r/modules/dummy_vars.htm</li>
 * <li>http://stackoverflow.com/questions/11952706/generate-a-dummy-variable-in-r</li>
 * <li>http://www.ats.ucla.edu/stat/mult_pkg/faq/general/dummy.htm</li>
 * <li>http://www.psychstat.missouristate.edu/multibook/mlt08m.html</li>
 * </ul>
 */
public class Factor<T> extends Vector<T> implements Serializable {

  /**
   * Instantiate a new Factor based on an existing DDF, given a column name. The column name is not verified for
   * correctness; any errors would only show up on actual usage.
   *
   * @param theDDF
   * @param theColumnName
   */
  public Factor(DDF theDDF, String theColumnName) {
    super(theDDF, theColumnName);
  }

  /**
   * Instantiate a new Factor with the given T array
   *
   * @param data
   * @param theColumnName
   * @throws DDFException
   */
  public Factor(String name, T[] data) throws DDFException {
    super(name, data);
  }

  /**
   * Instantiate a new Factor with the given T array
   *
   * @param data
   * @param theColumnName
   * @param engineName
   * @throws DDFException
   */
  public Factor(String name, T[] data, String engineName) throws DDFException {
    super(name, data, engineName);
  }


  private Map<String, Integer> mLevelMap;
  private List<String> mLevels;
  private Map<String, Integer> mLevelCounts;

  /**
   * Derived classes should call this to instantiate a synchronized level map for thread safety. Internally, we use
   * {@link LinkedHashMap} because it preserves the order based on insertion order, which is what we want.
   */
  protected Map<String, Integer> instantiateSynchronizedLevelMap() {
    return Collections.synchronizedMap(new LinkedHashMap<String, Integer>());
  }

  /**
   * The base implementation We use {@link LinkedHashMap} because it preserves the order based on insertion order, which
   * is what we want.
   * <p/>
   * Also see this informative R vignette: http://cran.r-project.org/web/packages/gdata/vignettes/mapLevels.pdf
   *
   * @throws DDFException
   */
  public Map<String, Integer> computeLevelMap() throws DDFException {
    // TODO: retrieve the list of levels from the underlying data, e.g.,

    return mLevelMap;
  }

  /**
   * Returns a String list of the named levels (sometimes referred to as "labels") in the factor
   *
   * @return
   * @throws DDFException
   */
  public List<String> getLevels() throws DDFException {
    return this.mLevels;
  }

  public Map<String, Integer> getLevelMap() throws DDFException {
    if (mLevelMap == null) mLevelMap = this.computeLevelMap();
    return mLevelMap;
  }

  /**
   * Typically, levels are automatically computed from the data, but in some rare instances, the user may want to
   * specify the levels explicitly, e.g., when the data column does not contain all the levels desired.
   *
   * @param levels
   * @param isOrdered a flag indicating whether the levels actually have "less than" and "greater than" left-to-right order
   *                  meaning
   * @throws DDFException
   */
  public void setLevels(List<String> levels, boolean isOrdered) throws DDFException {
    this.setLevels(levels, null, isOrdered);
  }

  public void setLevels(List<String> levels) throws DDFException {
    this.setLevels(levels, null, false); // with default values for level codes and isOrdered
  }

  /**
   * Similar to setLevels(levels, isOrdered),
   *
   * @param levels
   * @param codes
   * @param isOrdered a flag indicating whether the levels actually have "less than" and "greater than" left-to-right order
   *                  meaning
   * @throws DDFException
   */
  public void setLevels(List<String> levels, List<Integer> codes, boolean isOrdered) throws DDFException {
    if (levels == null || levels.isEmpty()) throw new DDFException("Levels cannot be null or empty");
    if (codes != null && codes.size() != levels.size()) throw new DDFException(String.format(
        "The number of levels is %d which does not match the number of codes %d", levels.size(), codes.size()));

    if (mLevelMap == null) mLevelMap = this.instantiateSynchronizedLevelMap();

    if (codes == null) {
      // Auto-create a 1-based level-code map
      codes = new ArrayList<Integer>();
      for (int i = 1; i <= levels.size(); i++) {
        codes.add(i);
      }
    }

    Iterator<String> levelIter = levels.iterator();
    Iterator<Integer> codeIter = codes.iterator();
    while (levelIter.hasNext()) {
      mLevelMap.put(levelIter.next(), codeIter.next());
    }
    this.mLevels = new ArrayList<String>(levels);
    this.setOrdered(isOrdered);
  }

  public void setLevelCounts(Map<String, Integer> levelCounts) {
    this.mLevelCounts = Collections.synchronizedMap(levelCounts);
  }

  public Map<String, Integer> getLevelCounts() throws DDFException {
    return this.mLevelCounts;
  }

  private boolean mIsOrdered = false;


  /**
   * @return whether this factor is ordered or not. I.e., do the levels have a meaning of "less-than" or "greater than"
   * each other.
   */
  public boolean isOrdered() {
    return mIsOrdered;
  }

  /**
   * @param isOrdered
   */
  public void setOrdered(boolean isOrdered) {
    this.mIsOrdered = isOrdered;
  }
}
