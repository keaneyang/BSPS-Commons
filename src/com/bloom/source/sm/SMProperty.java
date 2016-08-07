package com.bloom.source.sm;

import com.bloom.source.lib.prop.Property;

import java.util.Map;

public class SMProperty
  extends Property
{
  public final String ROW_DELIMITER = "rowdelimiter";
  public final String COLUMN_DELIMITER = "columndelimiter";
  public final String DEFAULT_ROW_DELIMITER = "\n";
  public final String DEFAULT_COLUMN_DELIMITER = ",";
  public final String DELIMITER_SPLIT_CHAR = "separator";
  public final String DEFAULT_DELIMITER_SPLIT_CHAR = ":";
  public final String QUOTECHAR = "quotecharacter";
  public final String QUOTE_SET = "quoteset";
  public final String ESCAPE_SEQUENCE = "escapesequence";
  public final String IGNORE_ROW_DELIMITER_IN_QUOTE = "IgnoreRowDelimiterInQuote";
  public final String DEFAULT_QUOTE_SET = Character.toString('"');
  public char commentcharacter;
  public String valueDelimiter;
  public String[] rowDelimiterList;
  public String[] columnDelimiterList;
  public String[] quoteSetList;
  public boolean escapeSequence;
  public boolean ignoreRowDelimiterInQueue;
  
  public SMProperty(Map<String, Object> mp)
  {
    super(mp);
    
    String delimiterSplitChar = (String)mp.get("separator");
    if (delimiterSplitChar == null)
    {
      getClass();delimiterSplitChar = ":";
    }
    this.commentcharacter = getChar(mp, "commentcharacter");
    
    String delimiter = "";
    Object obj = mp.get("rowdelimiter");
    if (obj != null)
    {
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
    else
    {
      getClass();delimiter = "\n";
    }
    this.rowDelimiterList = delimiter.split(delimiterSplitChar);
    
    obj = mp.get("columndelimiter");
    if (obj != null)
    {
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
    else
    {
      getClass();delimiter = ",";
    }
    if (mp.get("nocolumndelimiter") != null)
    {
      boolean noColumnDelimiter = ((Boolean)mp.get("nocolumndelimiter")).booleanValue();
      if (noColumnDelimiter) {
        delimiter = "";
      }
    }
    this.columnDelimiterList = delimiter.split(delimiterSplitChar);
    
    getClass();obj = mp.get("quoteset");
    if (obj == null)
    {
      getClass();obj = mp.get("quotecharacter");
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
    }
    else {
      delimiter = this.DEFAULT_QUOTE_SET;
    }
    this.quoteSetList = delimiter.split(delimiterSplitChar);
    
    getClass();obj = mp.get("escapesequence");
    if (obj != null) {
      this.escapeSequence = ((Boolean)obj).booleanValue();
    } else {
      this.escapeSequence = false;
    }
    getClass();obj = mp.get("IgnoreRowDelimiterInQuote");
    if (obj != null) {
      this.ignoreRowDelimiterInQueue = ((Boolean)obj).booleanValue();
    } else {
      this.ignoreRowDelimiterInQueue = false;
    }
  }
}
