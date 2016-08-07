package com.bloom.source.lib.meta;

import java.nio.ByteBuffer;

import com.bloom.source.lib.constant.Constant;

public class LEStringColumn
  extends Column
{
  public LEStringColumn()
  {
    setType(Constant.fieldType.STRING);
  }
  
  public int getLengthOfString(ByteBuffer buffer, int stringColumnLength)
  {
    byte[] strLength = new byte[stringColumnLength];
    int i = 0;
    long accum = 0L;
    for (i = 0; i < stringColumnLength; i++) {
      strLength[i] = buffer.get();
    }
    i = 0;
    for (int shiftBy = 0; shiftBy < stringColumnLength * 8; shiftBy += 8)
    {
      accum |= (strLength[i] & 0xFF) << shiftBy;
      i++;
    }
    setSize((int)accum);
    return (int)accum;
  }
  
  public Object getValue(byte[] rowdata, int offset, int length)
  {
    byte[] destination = new byte[length];
    
    System.arraycopy(rowdata, offset, destination, 0, length);
    
    return new String(destination);
  }
}
