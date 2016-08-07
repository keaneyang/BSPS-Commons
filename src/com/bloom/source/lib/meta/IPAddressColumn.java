package com.bloom.source.lib.meta;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Logger;

import com.bloom.source.lib.constant.Constant;

public class IPAddressColumn
  extends Column
{
  private static Logger logger = Logger.getLogger(IPAddressColumn.class);
  
  public IPAddressColumn()
  {
    setType(Constant.fieldType.IP);
  }
  
  public Object getValue(byte[] data, int offset, int length)
  {
    byte[] addressArray = new byte[length];
    System.arraycopy(data, offset, addressArray, 0, length);
    InetAddress ipAddress = null;
    try
    {
      ipAddress = InetAddress.getByAddress(addressArray);
    }
    catch (UnknownHostException e)
    {
      logger.warn("Unable to parse Ip Address from the given byte array");
    }
    return ipAddress.getHostAddress();
  }
}
