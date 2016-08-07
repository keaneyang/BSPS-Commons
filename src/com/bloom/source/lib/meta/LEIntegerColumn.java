package com.bloom.source.lib.meta;

import java.nio.ByteBuffer;

import com.bloom.source.lib.constant.Constant;

public class LEIntegerColumn
  extends Column
{
  public LEIntegerColumn()
  {
    setType(Constant.fieldType.INTEGER);
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    int i = 0;
    for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i++;
    }
    return Integer.valueOf((int)accum);
  }
  
  public int getLengthOfString(ByteBuffer buffer, int i)
  {
    return 0;
  }
}
