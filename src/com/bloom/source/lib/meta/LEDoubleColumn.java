package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;

public class LEDoubleColumn
  extends Column
{
  public LEDoubleColumn()
  {
    setType(Constant.fieldType.DOUBLE);
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    int i = 0;
    for (int shiftBy = 0; shiftBy < 64; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i++;
    }
    double retval = Double.longBitsToDouble(accum);
    return Double.valueOf(retval);
  }
}
