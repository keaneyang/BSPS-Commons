package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;

public class IntegerColumn
  extends Column
{
  public IntegerColumn()
  {
    setType(Constant.fieldType.INTEGER);
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + length) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    int i = length - 1;
    for (int shiftBy = 0; (shiftBy < 32) && (i >= 0); shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i--;
    }
    return Integer.valueOf((int)accum);
  }
  
  public static int getIntValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return -1;
    }
    long accum = 0L;
    int i = 3;
    for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i--;
    }
    return (int)accum;
  }
}
