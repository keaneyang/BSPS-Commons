package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;

public class FloatColumn
  extends Column
{
  public FloatColumn()
  {
    setType(Constant.fieldType.FLOAT);
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    int i = 3;
    for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i--;
    }
    int ret = (int)accum;
    float retVal = Float.intBitsToFloat(ret);
    return Float.valueOf(retVal);
  }
  
  public static float getFloatValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return -1.0F;
    }
    long accum = 0L;
    int i = 3;
    for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i--;
    }
    int ret = (int)accum;
    float retVal = Float.intBitsToFloat(ret);
    return retVal;
  }
}
