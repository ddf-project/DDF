package io.ddf2.datasource.schema;

import io.ddf2.DDFException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by sangdn on 1/29/16.
 */


public interface IFactor extends Cloneable{

  /**
   * The base implementation We use {@link LinkedHashMap} because it preserves the order based on insertion order, which
   * is what we want.
   * <p/>
   * Also see this informative R vignette: http://cran.r-project.org/web/packages/gdata/vignettes/mapLevels.pdf
   *
   * @throws DDFException
   */
  public Map<String, Integer> computeLevelMap() throws DDFException;

  /**
   * Returns a String list of the named levels (sometimes referred to as "labels") in the factor
   *
   * @return
   * @throws DDFException
   */
  public List<String> getLevels() throws DDFException;

  public Map<String, Integer> getLevelMap() throws DDFException;

  /**
   * Typically, levels are automatically computed from the data, but in some rare instances, the user may want to
   * specify the levels explicitly, e.g., when the data column does not contain all the levels desired.
   *
   * @param levels
   * @param isOrdered a flag indicating whether the levels actually have "less than" and "greater than" left-to-right order
   *                  meaning
   * @throws DDFException
   */
  public void setLevels(List<String> levels, boolean isOrdered) throws DDFException;

  public void setLevels(List<String> levels) throws DDFException;

  /**
   * Similar to setLevels(levels, isOrdered),
   *
   * @param levels
   * @param codes
   * @param isOrdered a flag indicating whether the levels actually have "less than" and "greater than" left-to-right order
   *                  meaning
   * @throws DDFException
   */
  public void setLevels(List<String> levels, List<Integer> codes, boolean isOrdered) throws DDFException;

  public void setLevelCounts(Map<String, Integer> levelCounts) throws DDFException;

  public Map<String, Integer> getLevelCounts() throws DDFException;


  /**
   * @return whether this factor is ordered or not. I.e., do the levels have a meaning of "less-than" or "greater than"
   * each other.
   */
  public boolean isOrdered();
  /**
   * @param isOrdered
   */
  public void setOrdered(boolean isOrdered);

}
