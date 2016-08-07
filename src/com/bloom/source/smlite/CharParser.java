package com.bloom.source.smlite;

import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public abstract class CharParser
{
  Logger logger = Logger.getLogger(CharParser.class);
  public static final int TABLE_SIZE = 65536;
  protected StateMachine stateMachine;
  SMProperty prop;
  String[] colDelimiter;
  String[] rowDelimiter;
  String[] quote;
  String[] nvpDelimiter;
  String[][] delimiterList;
  boolean hasRecordBeginSupport;
  Map<String, SMEvent> delimiterEventMap;
  Map<String, String> relatedDelimiters;
  private int maxDelimiterSize;
  protected String matchedDelimiter;
  
  public CharParser(StateMachine sm, SMProperty prop)
  {
    this.stateMachine = sm;
    this.prop = prop;
  }
  
  protected void init()
  {
    this.colDelimiter = this.prop.columnDelimiterList;
    this.rowDelimiter = this.prop.rowDelimiterList;
    this.quote = this.prop.quoteSetList;
    this.nvpDelimiter = this.prop.pairDelimiterList;
    
    this.delimiterList = new String[13][];
    this.delimiterEventMap = new HashMap();
    this.relatedDelimiters = new HashMap();
    
    initializeDelimiters();
  }
  
  protected void initializeDelimiters()
  {
    initializeDelimiter(this.prop.timeStamp, (short)9);
    
    initializeDelimiter(this.prop.columnDelimiterList, (short)1);
    initializeDelimiter(this.prop.rowDelimiterList, (short)2);
    if (this.prop.recordBeginWithTS) {
      initializeDelimiter(this.prop.timeStamp, (short)8);
    }
    initializeDelimiter(this.prop.recordBegin, (short)8);
    if ((this.prop.recordBegin != null) && (this.prop.recordBegin.length > 0)) {
      this.hasRecordBeginSupport = true;
    }
    initializeDelimiter(this.prop.recordEnd, (short)11);
    initializeDelimiter(this.prop.escapeList, (short)10);
    if (this.prop.commentcharacter != 0) {
      initializeDelimiter(this.prop.commentcharacter, (short)6);
    }
    initializeDelimiter(this.prop.pairDelimiterList, (short)12);
    if (this.prop.blockAsCompleteRecord) {
      initializeDelimiter('\000', (short)7);
    }
    initializeQuoteSet(this.prop.quoteSetList);
    
    finalizeDelimiterInit();
  }
  
  private void initializeDelimiter(char c, short eventType)
  {
    String[] tmp = new String[1];
    tmp[0] = ("" + c);
    initializeDelimiter(tmp, eventType);
  }
  
  protected void initializeDelimiter(String[] delimiter, short event)
  {
    for (int itr = 0; itr < delimiter.length; itr++) {
      if (this.maxDelimiterSize < delimiter[itr].length()) {
        this.maxDelimiterSize = delimiter[itr].length();
      }
    }
    this.delimiterList[event] = delimiter;
  }
  
  private void initializeQuoteSet(String[] quoteSet)
  {
    if (quoteSet == null) {
      return;
    }
    List<String> quote = new ArrayList();
    List<String> qBegin = new ArrayList();
    List<String> qEnd = new ArrayList();
    for (int itr = 0; (itr < quoteSet.length) && (quoteSet[itr].length() > 0); itr++)
    {
      int len = quoteSet[itr].length();
      if (len == 1)
      {
        quote.add("" + quoteSet[itr].charAt(0));
      }
      else
      {
        String begin = quoteSet[itr].substring(0, len / 2);
        String end = quoteSet[itr].substring(len / 2, len);
        if (len % 2 != 0) {
          this.logger.warn("Non even lengh quote-set is specfied. QuoteSet {" + quoteSet[itr] + "} will be split BeginQuote as {" + begin + "} and EndQuote as {" + end + "}");
        }
        qBegin.add(begin);
        qEnd.add(end);
        this.relatedDelimiters.put(begin, end);
      }
    }
    if (quote.size() > 0) {
      initializeDelimiter((String[])quote.toArray(new String[quote.size()]), (short)3);
    }
    if (qBegin.size() > 0)
    {
      initializeDelimiter((String[])qBegin.toArray(new String[qBegin.size()]), (short)4);
      initializeDelimiter((String[])qEnd.toArray(new String[qEnd.size()]), (short)5);
    }
  }
  
  public int addIntoLookupTable(char c)
  {
    return 0;
  }
  
  public int addIntoLookupTable(char c, int ordinal)
  {
    return ordinal;
  }
  
  public int addIntoGroupTable(String str)
  {
    return 0;
  }
  
  public int addIntoGroupTable(int groupId, char c)
  {
    return 0;
  }
  
  public int computeHashFor(String str)
  {
    return 0;
  }
  
  public abstract void ignoreEvents(short[] paramArrayOfShort);
  
  public abstract void ignoreEvents(short[] paramArrayOfShort, SMEvent paramSMEvent);
  
  public abstract void next()
    throws RecordException, AdapterException;
  
  public abstract boolean hasNext();
  
  public void validateProperty()
    throws AdapterException
  {}
  
  public void reset() {}
  
  public void setEventForGroup(int gId, char c, SMEvent event) {}
  
  public boolean hasRecordBeginSupport()
  {
    return this.hasRecordBeginSupport;
  }
  
  public int maxDelimiterLength()
  {
    return this.maxDelimiterSize;
  }
  
  public void finalizeDelimiterInit()
  {
    for (String entry : this.relatedDelimiters.keySet())
    {
      String relatedDel = (String)this.relatedDelimiters.get(entry);
      SMEvent event = (SMEvent)this.delimiterEventMap.get(entry);
      SMEvent relatedEvent = (SMEvent)this.delimiterEventMap.get(relatedDel);
      event.relatedEvent = relatedEvent;
    }
  }
  
  public void close() {}
}
