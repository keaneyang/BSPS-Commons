package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;

public class ByteColumn
  extends Column
{
  public ByteColumn()
  {
    setType(Constant.fieldType.BYTE);
  }
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    accum |= (rowData[offset] & 0xFF) << 0;
    
    return Byte.valueOf((byte)(int)accum);
  }
  
  public static byte getByteValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + 4) {
      return -1;
    }
    long accum = 0L;
    accum |= (rowData[offset] & 0xFF) << 0;
    
    return (byte)(int)accum;
  }
}
