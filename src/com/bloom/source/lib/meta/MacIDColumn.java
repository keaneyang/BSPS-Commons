package com.bloom.source.lib.meta;

import org.apache.log4j.Logger;

import com.bloom.source.lib.constant.Constant;

public class MacIDColumn
  extends Column
{
  private static Logger logger = Logger.getLogger(MacIDColumn.class);
  
  public MacIDColumn()
  {
    setType(Constant.fieldType.MACID);
  }
  
  public Object getValue(byte[] data, int offset, int length)
  {
    byte[] addressArray = new byte[length];
    System.arraycopy(data, offset, addressArray, 0, length);
    String str = "";
    str = str + String.format("%02X", new Object[] { Integer.valueOf(addressArray[0] & 0xFF) });
    for (int itr = 1; itr < length; itr++) {
      str = str + "-" + String.format("%02X", new Object[] { Integer.valueOf(addressArray[itr] & 0xFF) });
    }
    return str;
  }
}
