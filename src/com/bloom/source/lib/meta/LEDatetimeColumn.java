package com.bloom.source.lib.meta;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;

public class LEDatetimeColumn
  extends Column
{
  private SimpleDateFormat ft = new SimpleDateFormat("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
  
  public Object getValue(byte[] rowData, int offset, int length)
  {
    if (rowData.length < offset + length) {
      return Integer.valueOf(-1);
    }
    long accum = 0L;
    int i = 0;
    for (int shiftBy = 0; shiftBy < 64; shiftBy += 8)
    {
      accum |= (rowData[(offset + i)] & 0xFF) << shiftBy;
      i++;
    }
    Date dt = new Date(accum);
    return this.ft.format(dt);
  }
  
  public int getLengthOfString(ByteBuffer buffer, int i)
  {
    return 0;
  }
}
