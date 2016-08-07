package com.bloom.source.smlite;

import com.bloom.source.lib.intf.SMCallback;

public class SMEvent
  implements Cloneable
{
  public static final short RESET = 0;
  public static final short COLUMN = 1;
  public static final short ROW = 2;
  public static final short QUOTE = 3;
  public static final short QUOTE_BEGIN = 4;
  public static final short QUOTE_END = 5;
  public static final short COMMENT = 6;
  public static final short END_OF_BLOCK = 7;
  public static final short ROW_BEGIN = 8;
  public static final short TIME_STAMP = 9;
  public static final short ESCAPE_EVENT = 10;
  public static final short ROW_END = 11;
  public static final short END_OF_NVP = 12;
  public static final short MAX_STATES = 13;
  short eventState;
  public int currentPosition;
  public int length;
  String delimiter;
  public boolean isDateEvent;
  public String dateFormat;
  public int prefixLength;
  public int postfixLength;
  public boolean removePattern;
  public SMEvent relatedEvent;
  static int staticEventIdx;
  public int eventIdx;
  
  public SMEvent(SMEvent eventObj)
  {
    this.eventState = eventObj.eventState;
    this.delimiter = eventObj.delimiter;
    this.isDateEvent = eventObj.isDateEvent;
    this.length = eventObj.length;
    this.dateFormat = eventObj.dateFormat;
    this.prefixLength = eventObj.prefixLength;
    this.postfixLength = eventObj.postfixLength;
    this.removePattern = eventObj.removePattern;
    this.relatedEvent = eventObj.relatedEvent;
    this.eventIdx = (staticEventIdx++);
  }
  
  public SMEvent(short stateObj)
  {
    this.eventState = stateObj;
    this.length = 1;
    this.eventIdx = (++staticEventIdx);
  }
  
  public void delimiter(String del)
  {
    this.delimiter = del;
    this.length = this.delimiter.length();
  }
  
  public String delimiter()
  {
    return this.delimiter;
  }
  
  public void position(int currPos)
  {
    this.currentPosition = currPos;
  }
  
  public int position()
  {
    return this.currentPosition;
  }
  
  public void length(int len)
  {
    this.length = len;
  }
  
  public int length()
  {
    return this.length;
  }
  
  public short state()
  {
    return this.eventState;
  }
  
  void publishEvent(SMCallback callback)
  {
    callback.onEvent(this);
  }
  
  public Object clone()
    throws CloneNotSupportedException
  {
    SMEvent event = (SMEvent)super.clone();
    event.eventIdx = (++staticEventIdx);
    return event;
  }
}
