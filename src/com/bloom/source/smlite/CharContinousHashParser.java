package com.bloom.source.smlite;

import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class CharContinousHashParser
  extends CharParser
{
  private static Object lock = new Object();
  Logger logger = Logger.getLogger(CharContinousHashParser.class);
  final short END_OF_DELIMITER = 16384;
  final short BIT_MASK = 255;
  static final char CHAR_TO_REMOVE_PATTERN = '^';
  
  static class LookupReference
  {
    char[] ordinalHash;
    short[] groupHash;
    int[] groupToHash;
    SMEvent[] eventLookup;
    short[] hashLookup;
    short BIT_SHIFT = 8;
    int instanceCount;
    
    public LookupReference()
    {
      this.ordinalHash = new char[65536];
      this.hashLookup = new short[65536];
      this.groupHash = new short[2097152];
      this.groupToHash = new int[65536];
      this.eventLookup = new SMEvent[2097152];
    }
    
    public void clear()
    {
      this.ordinalHash = null;
      this.hashLookup = null;
      this.groupHash = null;
      this.groupToHash = null;
      this.eventLookup = null;
    }
  }
  
  String lookupKey = "";
  static Map<String, LookupReference> lookupMap;
  LookupReference reference;
  SMEvent[] localEventLookupTable;
  short lastCharCode;
  int hashValue;
  short lastCharIdx;
  short[] localHashLookup;
  short[] defaultHashLookup;
  short eventLookupIdx;
  List<String> entryList;
  List<Expression> listOfExpressions;
  List<String> mustOverride;
  Map<String, short[]> ignoreEventCache;
  Map<String, String> otherEventDelimiterCache;
  
  public CharContinousHashParser(StateMachine sm, SMProperty prop)
  {
    super(sm, prop);
  }
  
  protected synchronized void init()
  {
    this.hashValue = 0;
    this.lastCharIdx = 0;
    this.entryList = new ArrayList();
    this.listOfExpressions = new ArrayList();
    this.mustOverride = new ArrayList();
    
    this.ignoreEventCache = new HashMap();
    this.otherEventDelimiterCache = new HashMap();
    
    super.init();
  }
  
  public void initializeDelimiter(String[] delimters, short eventType)
  {
    if (delimters != null)
    {
      super.initializeDelimiter(delimters, eventType);
      for (int delItr = 0; (delItr < delimters.length) && (delimters[delItr].length() > 0); delItr++)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Initializging Delimiter[" + delimters[delItr] + "]");
        }
        this.lookupKey += delimters[delItr];
        SMEvent eventObj = EventFactory.createEvent(eventType);
        String delimiter = delimters[delItr];
        if ((delimiter.length() > 0) && (delimiter.charAt(0) == '^') && ((eventType == 8) || (eventType == 11)))
        {
          eventObj.removePattern = true;
          delimiter = delimiter.substring(1, delimiter.length());
        }
        if (eventObj.state() == 2) {
          ((RowEvent)eventObj).array(this.stateMachine.buffer());
        }
        eventObj.delimiter(delimiter);
        
        Expression exp = ExpressionBuilder.buildExpression(this, delimiter, eventObj);
        
        String[] additionalDel = exp.additionalDelimiters();
        if (additionalDel != null) {
          for (int itr = 0; itr < additionalDel.length; itr++) {
            this.mustOverride.add(additionalDel[itr]);
          }
        }
        this.listOfExpressions.add(exp);
        
        String[] tmp = exp.getDelimter();
        for (int itr = 0; itr < tmp.length; itr++) {
          this.delimiterEventMap.put(tmp[itr], eventObj);
        }
      }
    }
  }
  
  public void finalizeDelimiterInit()
  {
    synchronized (lock)
    {
      boolean needInitialization = false;
      if (lookupMap == null) {
        lookupMap = new TreeMap();
      }
      if ((this.reference = (LookupReference)lookupMap.get(this.lookupKey)) == null)
      {
        this.reference = new LookupReference();
        lookupMap.put(this.lookupKey, this.reference);
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Adding lookup table for {" + this.lookupKey + "}");
        }
        needInitialization = true;
      }
      if (needInitialization)
      {
        super.finalizeDelimiterInit();
        for (int expItr = 0; expItr < this.listOfExpressions.size(); expItr++)
        {
          Expression expp = (Expression)this.listOfExpressions.get(expItr);
          expp.initializeHashLookup();
        }
        this.reference.BIT_SHIFT = bitCount(this.lastCharCode);
        for (int expItr = 0; expItr < this.listOfExpressions.size(); expItr++)
        {
          Expression expp = (Expression)this.listOfExpressions.get(expItr);
          expp.initializeGroupLookup();
        }
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Last unique code generated : [" + this.lastCharCode + "]");
        }
      }
      this.defaultHashLookup = this.reference.hashLookup;
      this.localHashLookup = new short[65536];
      System.arraycopy(this.reference.hashLookup, 0, this.localHashLookup, 0, this.reference.hashLookup.length);
      this.localEventLookupTable = new SMEvent[SMEvent.staticEventIdx + 1];
      this.reference.instanceCount += 1;
    }
  }
  
  public int addIntoLookupTable(char c)
  {
    if (this.reference.hashLookup[((short)c)] == 0)
    {
      this.reference.hashLookup[((short)c)] = (this.lastCharCode = (short)(this.lastCharCode + 1));
      this.reference.ordinalHash[this.lastCharCode] = c;
      this.entryList.add("" + c);
      return this.lastCharCode;
    }
    return this.reference.hashLookup[((short)c)];
  }
  
  public int addIntoLookupTable(char c, int ordinal)
  {
    if (this.reference.ordinalHash[ordinal] != 0)
    {
      this.reference.hashLookup[((short)c)] = ((short)ordinal);
    }
    else
    {
      this.reference.ordinalHash[ordinal] = c;
      this.reference.hashLookup[((short)c)] = ((short)ordinal);
    }
    return ordinal;
  }
  
  public String hashToString(int hashValue)
  {
    String groupStr = "";
    if (this.reference.groupToHash[(hashValue >> this.reference.BIT_SHIFT)] != 0) {
      groupStr = hashToString(hashValue >> this.reference.BIT_SHIFT);
    } else {
      groupStr = groupStr + "" + this.reference.ordinalHash[(hashValue >> this.reference.BIT_SHIFT)];
    }
    return groupStr;
  }
  
  public String hashToString()
  {
    String groupStr = "";
    if (this.reference.groupToHash[(this.hashValue >> this.reference.BIT_SHIFT)] != 0) {
      groupStr = hashToString(this.reference.groupToHash[(this.hashValue >> this.reference.BIT_SHIFT)]);
    } else if (this.hashValue >> this.reference.BIT_SHIFT != 0) {
      groupStr = "" + this.reference.ordinalHash[((short)(this.hashValue >> this.reference.BIT_SHIFT))];
    }
    if (this.reference.groupToHash[(this.hashValue & 0xFF)] != 0) {
      return hashToString(this.reference.groupToHash[(this.hashValue & 0xFF)]);
    }
    groupStr = groupStr + "" + this.reference.ordinalHash[(this.hashValue & 0xFF)];
    return groupStr;
  }
  
  public void setEventForGroup(int gId, char c, SMEvent event)
  {
    int hash = gId << this.reference.BIT_SHIFT;
    hash |= this.reference.hashLookup[c];
    this.reference.eventLookup[hash] = event;
  }
  
  public void setEventForGroup(int gId, SMEvent event)
  {
    if (this.reference.groupHash[gId] != 0)
    {
      System.out.println("GroupID [" + gId + "] already has event");
      return;
    }
    this.reference.groupHash[gId] = ((short)(0x4000 | (this.eventLookupIdx = (short)(this.eventLookupIdx + 1))));
    this.reference.eventLookup[(0x4000 | this.eventLookupIdx)] = event;
  }
  
  public int addIntoGroupTable(int groupId, char c)
  {
    String groupStr = "";
    if (groupId != 0)
    {
      int hash = this.reference.groupToHash[groupId];
      if (hash == 0)
      {
        System.out.println("Invalid group id.");
        return 0;
      }
      groupStr = hashToString(hash);
      groupStr = groupStr + "" + c;
    }
    else
    {
      groupStr = "" + c;
    }
    groupId <<= this.reference.BIT_SHIFT;
    groupId |= this.reference.hashLookup[((short)c)];
    if (this.reference.groupHash[groupId] != 0) {
      return this.reference.groupHash[groupId];
    }
    this.reference.groupHash[groupId] = (this.lastCharCode = (short)(this.lastCharCode + 1));
    this.reference.groupToHash[this.lastCharCode] = groupId;
    this.entryList.add(groupStr);
    return this.lastCharCode;
  }
  
  public int addIntoGroupTable(String str)
  {
    int hashValue = computeHashFor(str);
    
    this.entryList.add(str);
    if (this.reference.groupHash[hashValue] == 0)
    {
      this.reference.groupHash[hashValue] = (this.lastCharCode = (short)(this.lastCharCode + 1));
      this.reference.groupToHash[this.lastCharCode] = hashValue;
    }
    else
    {
      return this.reference.groupHash[hashValue];
    }
    return this.lastCharCode;
  }
  
  private short bitCount(int uniqueCode)
  {
    short cnt = 0;
    int bit = 1;
    while (bit <= uniqueCode)
    {
      bit <<= 1;
      cnt = (short)(cnt + 1);
    }
    return cnt;
  }
  
  public int addIntoGroupTable(String str, int gId)
  {
    int hashValue = computeHashFor(str);
    this.entryList.add(str);
    if (this.reference.groupHash[hashValue] == 0) {
      this.reference.groupHash[hashValue] = ((short)gId);
    } else if (this.reference.groupHash[hashValue] != gId) {
      this.logger.warn("Seen clash with other group [" + str + "] GID [" + gId + "] existing GID [" + this.reference.groupHash[hashValue] + "]");
    }
    return gId;
  }
  
  public void ignoreEvents(short[] events, SMEvent excludeEvent)
  {
    ignoreEvents(events);
    if (excludeEvent != null)
    {
      String eventString = "" + Arrays.hashCode(events);
      String additionalDelString = "";
      
      additionalDelString = additionalDelString + "" + excludeEvent.delimiter;
      eventString = eventString + additionalDelString;
      
      short[] tmpHash = (short[])this.ignoreEventCache.get(eventString);
      if (tmpHash == null)
      {
        tmpHash = new short[65536];
        System.arraycopy(this.localHashLookup, 0, tmpHash, 0, this.localHashLookup.length);
        String delimiter = excludeEvent.delimiter;
        for (int len = 0; len < delimiter.length(); len++) {
          if (delimiter.charAt(len) != 0) {
            tmpHash[delimiter.charAt(len)] = this.reference.hashLookup[delimiter.charAt(len)];
          } else {
            this.logger.warn("Got NULL character in delimiter");
          }
        }
        this.ignoreEventCache.put(eventString, tmpHash);
      }
      this.localHashLookup = tmpHash;
    }
  }
  
  public void ignoreEvents(short[] events)
  {
    if (events == null)
    {
      this.localHashLookup = this.defaultHashLookup;
    }
    else
    {
      String eventString = "" + Arrays.hashCode(events);
      
      short[] tmpHash = (short[])this.ignoreEventCache.get(eventString);
      if (tmpHash == null)
      {
        tmpHash = new short[65536];
        System.arraycopy(this.defaultHashLookup, 0, tmpHash, 0, this.defaultHashLookup.length);
        
        String otherEventDelimiterString = (String)this.otherEventDelimiterCache.get(eventString);
        if (otherEventDelimiterString == null)
        {
          StringBuilder otherEventDelimiterStringBuilder = new StringBuilder();
          for (int stateItr = 0; stateItr < 13; stateItr++)
          {
            boolean skip = false;
            for (int intItr = 0; intItr < events.length; intItr++) {
              if (events[intItr] == stateItr)
              {
                skip = true;
                break;
              }
            }
            if ((!skip) && (this.delimiterList[stateItr] != null)) {
              for (int delItr = 0; delItr < this.delimiterList[stateItr].length; delItr++) {
                otherEventDelimiterStringBuilder.append(this.delimiterList[stateItr][delItr]);
              }
            }
          }
          otherEventDelimiterString = otherEventDelimiterStringBuilder.toString();
          this.otherEventDelimiterCache.put(eventString, otherEventDelimiterString);
        }
        StringBuilder eventDelimitersBuilder = new StringBuilder();
        for (int stateItr = 0; stateItr < events.length; stateItr++) {
          if (this.delimiterList[events[stateItr]] != null) {
            for (int delItr = 0; delItr < this.delimiterList[events[stateItr]].length; delItr++) {
              eventDelimitersBuilder.append(this.delimiterList[events[stateItr]][delItr]);
            }
          }
        }
        String eventDelimiters = eventDelimitersBuilder.toString();
        for (int itr = 0; itr < eventDelimiters.length(); itr++) {
          if (otherEventDelimiterString.indexOf(eventDelimiters.charAt(itr)) == -1) {
            tmpHash[eventDelimiters.charAt(itr)] = 0;
          }
        }
        this.ignoreEventCache.put(eventString, tmpHash);
      }
      this.localHashLookup = tmpHash;
    }
  }
  
  public int computeHashFor(String str)
  {
    int hash = 0;
    for (int itr = 0; itr < str.length(); itr++)
    {
      hash <<= this.reference.BIT_SHIFT;
      if (this.reference.hashLookup[str.charAt(itr)] == 0) {
        hash |= addIntoLookupTable(str.charAt(itr));
      } else {
        hash |= this.reference.hashLookup[str.charAt(itr)];
      }
    }
    return hash;
  }
  
  protected boolean evaluateChar(char charIdx)
  {
    short hashCode;
    if ((hashCode = this.localHashLookup[charIdx]) != 0)
    {
      this.hashValue <<= this.reference.BIT_SHIFT;
      this.hashValue |= hashCode;
      SMEvent event;
      if (((event = this.reference.eventLookup[this.hashValue]) != null) || ((event = this.reference.eventLookup[hashCode]) != null))
      {
        SMEvent localEvent = this.localEventLookupTable[event.eventIdx];
        if (localEvent == null)
        {
          try
          {
            localEvent = (SMEvent)event.clone();
          }
          catch (CloneNotSupportedException e)
          {
            e.printStackTrace();
          }
          this.localEventLookupTable[event.eventIdx] = localEvent;
        }
        this.hashValue = 0;
        this.stateMachine.publishEvent(localEvent);
        if (this.stateMachine.canBreak()) {
          return true;
        }
      }
      else
      {
        int groupCode;
        if ((groupCode = this.reference.groupHash[this.hashValue]) != 0)
        {
          this.lastCharIdx = 0;
          this.hashValue = groupCode;
        }
        else
        {
          this.hashValue = hashCode;
        }
      }
    }
    else
    {
      this.hashValue = 0;
    }
    return false;
  }
  
  public void next()
    throws RecordException, AdapterException
  {
    while (evaluateChar(this.stateMachine.getChar()) != true) {}
  }
  
  public boolean hasNext()
  {
    return false;
  }
  
  public void reset()
  {
    this.hashValue = 0;
  }
  
  public void close()
  {
    synchronized (lock)
    {
      if (this.reference != null)
      {
        this.reference.instanceCount -= 1;
        if (this.reference.instanceCount == 0)
        {
          if (this.logger.isDebugEnabled()) {
            this.logger.debug("No instance refereing to this lookup table, clearing it. Key {" + lookupMap + "}");
          }
          lookupMap.remove(this.lookupKey);
          this.reference.clear();
          this.reference = null;
        }
        this.localHashLookup = null;
        this.defaultHashLookup = null;
        this.entryList = null;
        this.listOfExpressions = null;
        this.mustOverride = null;
        this.ignoreEventCache = null;
        this.otherEventDelimiterCache = null;
      }
    }
  }
}
