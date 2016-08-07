package com.bloom.source.lib.utils;

import com.bloom.source.lib.constant.Constant;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.log4j.Logger;

public class Utils
{
  static String intToString = null;
  static byte[] stringToBytes = null;
  static int stringLength = 0;
  static byte[] arrayOfString = null;
  static int stringToInt;
  private static Logger logger = Logger.getLogger(Utils.class);
  
  public static byte[] convertIntegerToStringBytes(int integer)
  {
    intToString = Integer.toString(integer);
    
    int byteArrayLength = (intToString + Constant.STRING_LENGTH_SENTINEL).getBytes().length;
    
    stringToBytes = new byte[byteArrayLength];
    
    stringToBytes = (intToString + Constant.STRING_LENGTH_SENTINEL).getBytes();
    
    return stringToBytes;
  }
  
  public static int convertStringBytesToInteger(byte[] byteArray)
  {
    int arrayLength = byteArray.length;
    int strLen = 0;
    for (int i = 0; i < arrayLength; i++) {
      if (byteArray[i] != Constant.STRING_LENGTH_SENTINEL)
      {
        strLen++;
      }
      else
      {
        stringLength = strLen;
        break;
      }
    }
    arrayOfString = new byte[stringLength];
    
    System.arraycopy(byteArray, 0, arrayOfString, 0, stringLength);
    
    String byteToString = new String(arrayOfString);
    
    stringToInt = Integer.parseInt(byteToString);
    
    return stringToInt;
  }
  
  public static int getSizeLength()
  {
    return stringLength + 1;
  }
  
  public static short convertToShort(ByteBuffer buf)
  {
    byte byte0 = buf.get();
    byte byte1 = buf.get();
    int iHigh;
    int iLow;
    if (isBIGEndian())
    {
       iLow = byte1;
      iHigh = byte0;
    }
    else
    {
      iLow = byte0;
      iHigh = byte1;
    }
    int retval = iHigh << 8 | 0xFF & iLow;
    
    return (short)retval;
  }
  
  public static String convertToString(byte[] rowdata, int offset, int length)
  {
    byte[] destination = new byte[length];
    
    System.arraycopy(rowdata, offset, destination, 0, length);
    
    return new String(destination);
  }
  
  public static int convertToInteger(byte[] rowdata, int offset, int length)
  {
    if (rowdata.length < offset + 4) {
      return -1;
    }
    long accum = 0L;
    int i = 0;
    for (int shiftBy = 0; shiftBy < 32; shiftBy += 8)
    {
      accum |= (rowdata[(offset + i)] & 0xFF) << shiftBy;
      i++;
    }
    return (int)accum;
  }
  
  public static double convertToDouble(byte[] rowdata, int offset, int length)
  {
    long accum = 0L;
    int i = 0;
    for (int shiftBy = 0; shiftBy < 64; shiftBy += 8)
    {
      accum |= (rowdata[(offset + i)] & 0xFF) << shiftBy;
      i++;
    }
    double retval = Double.longBitsToDouble(accum);
    return retval;
  }
  
  static boolean isBIGEndian()
  {
    ByteOrder b = ByteOrder.nativeOrder();
    if (b.equals(ByteOrder.BIG_ENDIAN)) {
      return true;
    }
    return false;
  }
  
  public static boolean isBufferEmpty(Buffer buffer)
  {
    if ((buffer.position() == 0) && (buffer.limit() == 0)) {
      return true;
    }
    return false;
  }
  
  public static String getDBType(String dbURL)
  {
    String schema = "";
    try
    {
      URI jURI = new URI(dbURL.substring(5));
      schema = jURI.getScheme();
    }
    catch (URISyntaxException e)
    {
      String errMsg = "Invalid URL {" + dbURL + "}";
      logger.error(errMsg, e);
    }
    return schema;
  }
}
