package com.bloom.source.smlite;

import com.bloom.source.lib.prop.Property;

import java.util.Map;

public class SMProperty
  extends Property
{
  public char commentcharacter;
  public String[] rowDelimiterList;
  public String[] columnDelimiterList = new String[0];
  public String[] quoteSetList;
  public String[] escapeList;
  public String[] pairDelimiterList;
  public boolean escapeSequence;
  public boolean ignoreRowDelimiterInQuote;
  public boolean blockAsCompleteRecord;
  public String[] timeStamp;
  public String[] recordBegin;
  public String[] recordEnd;
  public String delimiterSplitChar;
  public boolean trimQuote;
  public boolean recordBeginWithTS;
  
  public SMProperty(Map<String, Object> mp)
  {
    super(mp);
    
    this.delimiterSplitChar = ((String)mp.get("separator"));
    if (this.delimiterSplitChar == null) {
      this.delimiterSplitChar = ":";
    }
    this.commentcharacter = getChar(mp, "commentcharacter");
    
    String delimiter = "";
    Object obj = mp.get("rowdelimiter");
    if (obj != null) {
      if ((obj instanceof String))
      {
        delimiter = (String)obj;
      }
      else
      {
        char del = ((Character)mp.get("rowdelimiter")).charValue();
        delimiter = Character.toString(del);
      }
    }
    this.rowDelimiterList = delimiter.split(this.delimiterSplitChar);
    
    obj = mp.get("columndelimiter");
    if (obj != null) {
      if ((obj instanceof String))
      {
        delimiter = (String)obj;
      }
      else
      {
        char del = ((Character)mp.get("columndelimiter")).charValue();
        delimiter = Character.toString(del);
      }
    }
    boolean noColumnDelimiter = false;
    if (mp.get("nocolumndelimiter") != null) {
      noColumnDelimiter = ((Boolean)mp.get("nocolumndelimiter")).booleanValue();
    }
    if (!noColumnDelimiter) {
      this.columnDelimiterList = delimiter.split(this.delimiterSplitChar);
    }
    obj = mp.get("pairdelimiter");
    if (obj != null)
    {
      delimiter = (String)obj;
      this.pairDelimiterList = delimiter.split(this.delimiterSplitChar);
    }
    else
    {
      this.pairDelimiterList = null;
    }
    obj = mp.get("quoteset");
    if (obj == null) {
      obj = mp.get("quotecharacter");
    }
    if (obj != null)
    {
      if ((obj instanceof String))
      {
        delimiter = (String)obj;
      }
      else
      {
        char del = ((Character)obj).charValue();
        delimiter = Character.toString(del);
      }
      this.quoteSetList = delimiter.split(this.delimiterSplitChar);
    }
    obj = mp.get("escapesequence");
    if (obj != null) {
      this.escapeSequence = ((Boolean)obj).booleanValue();
    } else {
      this.escapeSequence = false;
    }
    obj = mp.get("IgnoreRowDelimiterInQuote");
    if (obj != null) {
      this.ignoreRowDelimiterInQuote = ((Boolean)obj).booleanValue();
    } else {
      this.ignoreRowDelimiterInQuote = false;
    }
    this.blockAsCompleteRecord = getBoolean("blockAsCompleteRecord", false);
    String tmp = getString("TimeStamp", "");
    if (!tmp.isEmpty()) {
      this.timeStamp = tmp.split(this.delimiterSplitChar);
    }
    tmp = getString("RecordBegin", "");
    if (!tmp.isEmpty()) {
      this.recordBegin = tmp.split(this.delimiterSplitChar);
    }
    delimiter = getString("escapecharacter", "");
    if (!delimiter.isEmpty()) {
      this.escapeList = delimiter.split(this.delimiterSplitChar);
    }
    this.trimQuote = getBoolean("trimquote", true);
    
    tmp = getString("RecordEnd", "");
    if (!tmp.isEmpty()) {
      this.recordEnd = tmp.split(this.delimiterSplitChar);
    }
    this.recordBeginWithTS = getBoolean("RecordBeginWithTimeStamp", false);
  }
}
