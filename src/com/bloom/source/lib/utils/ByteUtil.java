package com.bloom.source.lib.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.log4j.Logger;

public class ByteUtil
{
  static Logger logger = Logger.getLogger(ByteUtil.class);
  
  public static int bytesToShort(byte[] bytes, int startPos)
  {
    return ((bytes[startPos] & 0xFF) << 8) + (bytes[(startPos + 1)] & 0xFF);
  }
  
  private static String pad(String str, int len, String padWith)
  {
    if (str.length() < len) {
      for (int i = str.length(); i < len; i++) {
        str = str + padWith;
      }
    }
    return str;
  }
  
  public static String bytesToHex(byte[] bytes, int offset, int len)
  {
    String hex = "";
    for (int i = offset; i < offset + len; i++)
    {
      String tmpHex = Integer.toHexString(bytes[i] & 0xFF).toUpperCase();
      if (tmpHex.length() == 1) {
        tmpHex = "0" + tmpHex;
      }
      hex = hex + tmpHex;
    }
    return hex;
  }
  
  public static String bytesToAscii(byte[] bytes, int offset, int len)
  {
    String str = "";
    for (int i = offset; i < offset + len; i++) {
      if ((bytes[i] & 0xFF) < 32) {
        str = str + ".";
      } else {
        str = str + new String(bytes, i, 1);
      }
    }
    return str;
  }
  
  public static String bytesToAsciiAndHex(byte[] bytes, int offset, int len)
  {
    return pad(bytesToAscii(bytes, offset, len) + "  (x" + bytesToHex(bytes, offset, len) + ")", 15, " ");
  }
  
  public static String bytesToShortAndHex(byte[] bytes, int offset, int len)
  {
    int shortVal = 0;
    if (len == 1) {
      shortVal = bytes[offset] & 0xFF;
    } else if (len == 2) {
      shortVal = bytesToShort(bytes, offset);
    }
    return pad(shortVal + "  (x" + bytesToHex(bytes, offset, len) + ")", 15, " ");
  }
  
  public static long bytesToLong(byte[] bytes, int offset, int len)
  {
    long value = 0L;
    for (int i = offset; i < offset + len; i++)
    {
      value <<= 8;
      value |= bytes[i] & 0xFF;
    }
    return value;
  }
  
  public static Long bytesToLongObject(byte[] bytes, int pos, int len)
  {
    long val = 0L;
    
    val = bytesToLong(bytes, pos, len);
    
    return new Long(val);
  }
  
  public static String bytesToLongString(byte[] bytes, int offset, int len)
  {
    return pad(String.valueOf(bytesToLong(bytes, offset, len)), 15, " ");
  }
  
  private static Number longToNumberWithScale(long value, int scale)
  {
    if (scale > 0)
    {
      double doubleValue = value * 1.0D / Math.pow(10.0D, scale * 1.0D);
      return new Double(doubleValue);
    }
    return new Long(value);
  }
  
  public static Number bytesToNumber(byte[] bytes, int offset, int len, int fraction)
  {
    long value = bytesToLong(bytes, offset, len);
    return longToNumberWithScale(value, fraction);
  }
  
  public static Number bytesInDPIEEFPToNumber(byte[] bytes, int offset, int len, int fraction)
  {
    long value = bytesToLong(bytes, offset, len);
    
    double doubleValue = Double.longBitsToDouble(value);
    if (fraction == 0) {
      return new Long(value);
    }
    return new Double(doubleValue);
  }
  
  public static String bytesToString(byte[] bytes, int offset, int len)
  {
    return new String(bytes, offset, len);
  }
  
  public static Number bytesAsFixedPrecisionStringToNumber(byte[] bytes, int offset, int len, int scale)
  {
    String s = new String(bytes, offset, len);
    long l = Long.parseLong(s);
    return longToNumberWithScale(l, scale);
  }
  
  public static Number bytesAsFloatStringToNumber(byte[] bytes, int offset, int len, int scale)
  {
    String s = new String(bytes, offset, len);
    return new Double(s);
  }
  
  private static SimpleDateFormat sdf_23 = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss.SSS");
  private static SimpleDateFormat sdf_19 = new SimpleDateFormat("yyyy-MM-dd:HH:mm:ss");
  private static SimpleDateFormat sdf_23s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private static SimpleDateFormat sdf_19s = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
  private static SimpleDateFormat sdf_10 = new SimpleDateFormat("yyyy-MM-dd");
  
  static
  {
    sdf_23.setLenient(true);
    sdf_19.setLenient(true);
    sdf_23s.setLenient(true);
    sdf_19s.setLenient(true);
    sdf_10.setLenient(true);
  }
  
  public static Date bytesContainingAsciiDateToDate2(byte[] bytes, int pos, int len)
  {
    String dateString = new String(bytes, pos, len).trim();
    len = dateString.length();
    if (len > 23)
    {
      dateString = dateString.substring(0, 23);
      len = 23;
    }
    try
    {
      if (len > 10)
      {
        if (dateString.charAt(10) == ' ')
        {
          if (len == 23) {
            return sdf_23s.parse(dateString);
          }
          if (len == 19) {
            return sdf_19s.parse(dateString);
          }
        }
        else
        {
          if (len == 23) {
            return sdf_23.parse(dateString);
          }
          if (len == 19) {
            return sdf_19.parse(dateString);
          }
        }
      }
      else {
        return sdf_10.parse(dateString);
      }
    }
    catch (Exception e)
    {
      logger.error("Couldn't parse Date: " + dateString, e);
    }
    return null;
  }
  
  public static Date bytesContainingAsciiDateToDate(byte[] bytes, int pos, int len)
  {
    char zero = '0';
    int twoDigitZero = zero * '\013';
    int threeDigitZero = zero * 'o';
    int fourDigitZero = zero * '��';
    int year = bytes[pos] * 1000 + bytes[(pos + 1)] * 100 + bytes[(pos + 2)] * 10 + bytes[(pos + 3)] - fourDigitZero;
    int month = bytes[(pos + 5)] * 10 + bytes[(pos + 6)] - twoDigitZero;
    int day = bytes[(pos + 8)] * 10 + bytes[(pos + 9)] - twoDigitZero;
    
    int hour = 0;
    int min = 0;
    int sec = 0;
    int millis = 0;
    if ((len > 10) && (bytes[(pos + 11)] != 32))
    {
      hour = bytes[(pos + 11)] * 10 + bytes[(pos + 12)] - twoDigitZero;
      min = bytes[(pos + 14)] * 10 + bytes[(pos + 15)] - twoDigitZero;
      sec = bytes[(pos + 17)] * 10 + bytes[(pos + 18)] - twoDigitZero;
      if ((len > 19) && (bytes[(pos + 20)] != 32)) {
        millis = bytes[(pos + 20)] * 100 + bytes[(pos + 21)] * 10 + bytes[(pos + 22)] - threeDigitZero;
      }
    }
    int[] daysByMonth = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334 };
    int[] daysByMonthLY = { 0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335 };
    int daysInYear = 365;
    int daysInLY = 366;
    int secsPerDay = 86400;
    int secsPerHour = 3600;
    int secsPerMinute = 60;
    
    long unixTime = 0L;
    for (int i = 1970; i < year; i++) {
      unixTime += (i % 4 == 0 ? daysInLY : i % 100 == 0 ? daysInYear : i % 400 == 0 ? daysInLY : daysInYear);
    }
    unixTime += (year % 4 == 0 ? daysByMonthLY[(month - 1)] : year % 100 == 0 ? daysByMonth[(month - 1)] : year % 400 == 0 ? daysByMonthLY[(month - 1)] : daysByMonth[(month - 1)]);
    unixTime += day - 1;
    unixTime *= secsPerDay;
    unixTime += hour * secsPerHour;
    unixTime += min * secsPerMinute;
    unixTime += sec;
    unixTime *= 1000L;
    unixTime += millis;
    
    return new Date(unixTime);
  }
  
  private static SimpleDateFormat sdf_julian_ts = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
  
  public static Date bytesContainingJulianLongToDate(byte[] bytes, int offset, int len)
  {
    long julianLong = bytesToLong(bytes, offset, len);
    julianLong /= 1000L;
    long milli = julianLong % 1000L;
    julianLong /= 1000L;
    
    long secs = julianLong % 60L;
    julianLong /= 60L;
    long mins = julianLong % 60L;
    julianLong /= 60L;
    long hours = julianLong % 24L;
    julianLong /= 24L;
    long daysSince1_1_4713BC_to_1_1_1970 = 2440588L;
    
    julianLong -= daysSince1_1_4713BC_to_1_1_1970;
    julianLong *= 24L;
    julianLong += hours + 12L;
    julianLong *= 60L;
    julianLong += mins;
    julianLong *= 60L;
    julianLong += secs;
    julianLong *= 1000L;
    julianLong += milli;
    return new Date(julianLong);
  }
  
  public static String bytesContainingJulianLongToString(byte[] bytes, int offset, int len)
  {
    long julianLong = bytesToLong(bytes, offset, len);
    long micro = julianLong % 1000L;
    
    return sdf_julian_ts.format(bytesContainingJulianLongToDate(bytes, offset, len)) + "." + micro;
  }
  
  public static byte[] byteJoin(byte[] first, byte[] second)
  {
    byte[] bytes = new byte[first.length + second.length];
    System.arraycopy(first, 0, bytes, 0, first.length);
    System.arraycopy(second, 0, bytes, first.length, second.length);
    
    return bytes;
  }
}
