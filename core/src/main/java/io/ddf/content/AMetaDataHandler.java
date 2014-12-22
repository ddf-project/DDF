/**
 *
 */
package io.ddf.content;


import io.ddf.DDF;
import io.ddf.exception.DDFException;
import io.ddf.misc.ADDFFunctionalGroupHandler;

import java.util.HashMap;
import java.util.UUID;

public abstract class AMetaDataHandler extends ADDFFunctionalGroupHandler
    implements IHandleMetaData {


  public AMetaDataHandler(DDF theDDF) {
    super(theDDF);
  }

  private UUID mId = UUID.randomUUID();

  @Override
  public UUID getId() {
    return mId;
  }

  @Override
  public void setId(UUID id) {
    mId = id;
  }

  private long mNumRows = 0L;
  private boolean bNumRowsIsValid = false;

  /**
   * Each implementation needs to come up with its own way to compute the row
   * count.
   *
   * @return row count of a DDF
   */
  protected abstract long getNumRowsImpl() throws DDFException;

  /**
   * Called to assert that the row count needs to be recomputed at next access
   */

  protected void invalidateNumRows() {
    bNumRowsIsValid = false;
  }

  @Override
  public long getNumRows() throws DDFException {
    if (!bNumRowsIsValid) {
      mNumRows = this.getNumRowsImpl();
      //      bNumRowsIsValid = true;
    }
    return mNumRows;
  }

  private HashMap<Integer, ICustomMetaData> mCustomMetaDatas;

  public ICustomMetaData getCustomMetaData(int idx) {
    return mCustomMetaDatas.get(idx);
  }

  public void setCustomMetaData(ICustomMetaData customMetaData) {
    mCustomMetaDatas.put(customMetaData.getColumnIndex(), customMetaData);
  }

  public HashMap<Integer, ICustomMetaData> getListCustomMetaData() {
    return mCustomMetaDatas;
  }

  public static interface ICustomMetaData {

    public double[] buildCoding(String value);

    public double get(String value, int idx);

    public int getColumnIndex();
  }

}
