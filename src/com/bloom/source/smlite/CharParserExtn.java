package com.bloom.source.smlite;

import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class CharParserExtn
  extends CharParser
{
  Logger logger = Logger.getLogger(CharParserExtn.class);
  protected final short START_OF_DELIMITER = 16;
  protected final short END_OF_DELIMITER = 32;
  protected final short PART_OF_DELIMITER = 64;
  protected short[] hash;
  long hashValue;
  short eventType;
  Map<Integer, String[]> eventToDelimiter;
  short[] specialCharHash;
  short[] specialChars;
  Map<Long, SMEvent> eventMap;
  Map<String, SMEvent> delimiterToEventMap;
  Map<String, SMEvent> defaultDelimiterToEventMap;
  List<String> possibleCombinations;
  List<String> allDelimiters;
  Map<Integer, String[]> eventToDelimiterMap;
  Map<String, short[]> eventToHashMap;
  Map<Integer, String[]> specialDelimiters;
  String otherChars;
  protected short lastCharValue = 0;
  protected short[] charToCodeMap;
  protected char[] codeToCharMap;
  short[] defaultEventType = { 2, 1, 3, 4, 5 };
  long bitMask;
  int bitCnt;
  int charMask;
  int maxPossibleDelimiterLength;
  int maxPossibleCombinationLength;
  int groupStartId;
  
  public CharParserExtn(StateMachine sm, SMProperty prop)
  {
    super(sm, prop);
  }
  
  protected void init()
  {
    super.init();
    initializeInternalDelimiters();
    initializeEventToDelimiterMap();
    initLookupTable();
  }
  
  private int maxPossibleDelimiterLength()
  {
    int len = 0;
    for (int itr = 0; itr < this.colDelimiter.length; itr++) {
      if (this.colDelimiter[itr].length() > len) {
        len = this.colDelimiter[itr].length();
      }
    }
    for (int itr = 0; itr < this.rowDelimiter.length; itr++) {
      if (this.rowDelimiter[itr].length() > len) {
        len = this.rowDelimiter[itr].length();
      }
    }
    return len * 2 - 1;
  }
  
  public void validateProperty()
    throws AdapterException
  {
    String delimiters = "";
    for (int itr = 0; itr < this.colDelimiter.length; itr++) {
      if (delimiters.indexOf(this.colDelimiter[itr]) != -1)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Column delimiter [" + this.colDelimiter[itr] + "] is over-lapped with other delimiter, you may not get this event");
        }
      }
      else {
        delimiters = delimiters + "," + this.colDelimiter[itr];
      }
    }
    for (int itr = 0; itr < this.rowDelimiter.length; itr++) {
      if (delimiters.indexOf(this.colDelimiter[itr]) != -1)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Row delimiter [" + this.rowDelimiter[itr] + "] is over-lapped with other delimiter, you may not get this event");
        }
      }
      else {
        delimiters = delimiters + "," + this.rowDelimiter[itr];
      }
    }
    this.hashValue = 1L;
    this.hashValue <<= this.maxPossibleDelimiterLength * this.bitCnt;
    if (this.hashValue < 0L)
    {
      if (this.logger.isDebugEnabled())
      {
        this.logger.debug("Bit Count : [" + this.bitCnt + "]");
        this.logger.debug("Max combination length [" + this.maxPossibleDelimiterLength + "]");
        this.logger.debug("Parser couldn't handle the given combination of delimiter");
      }
      throw new AdapterException("Parser couldn't handle the given combination of delimiter");
    }
  }
  
  private void initializeEventToDelimiterMap()
  {
    if (this.eventToDelimiterMap == null) {
      this.eventToDelimiterMap = new HashMap();
    }
    this.eventToDelimiterMap.put(Integer.valueOf(1), this.prop.columnDelimiterList);
    this.eventToDelimiterMap.put(Integer.valueOf(2), this.prop.rowDelimiterList);
    this.eventToDelimiterMap.put(Integer.valueOf(3), this.prop.quoteSetList);
  }
  
  private short[] createHash(short[] eventType)
  {
    StringBuilder eventStringBuilder = new StringBuilder();
    short[] hash = null;
    if (eventType == null) {
      eventType = this.defaultEventType;
    }
    for (int eventItr = 0; eventItr < eventType.length; eventItr++) {
      eventStringBuilder.append("," + Integer.toString(eventType[eventItr]));
    }
    String eventString = eventStringBuilder.toString();
    if (this.eventToHashMap == null) {
      this.eventToHashMap = new HashMap();
    }
    if ((hash = (short[])this.eventToHashMap.get(eventString)) == null)
    {
      short[] newHash = new short[65536];
      
      boolean proceed = true;
      for (Map.Entry<Integer, String[]> entry : this.eventToDelimiterMap.entrySet())
      {
        int event = ((Integer)entry.getKey()).intValue();
        proceed = false;
        for (int eventItr = 0; eventItr < eventType.length; eventItr++) {
          if (eventType[eventItr] != event)
          {
            proceed = true;
            break;
          }
        }
        if (proceed)
        {
          String[] delimiterList = (String[])this.eventToDelimiterMap.get(Integer.valueOf(event));
          
          String[] tmp = (String[])this.specialDelimiters.get(Integer.valueOf(event));
          if (tmp != null)
          {
            List<String> list = new ArrayList();
            if (delimiterList != null) {
              for (int dItr = 0; dItr < delimiterList.length; dItr++) {
                list.add(delimiterList[dItr]);
              }
            }
            for (int dItr = 0; dItr < tmp.length; dItr++) {
              list.add(tmp[dItr]);
            }
            delimiterList = (String[])list.toArray(new String[list.size()]);
          }
          if (delimiterList != null) {
            for (int strItr = 0; strItr < delimiterList.length; strItr++)
            {
              int len = delimiterList[strItr].length();
              String delimiter = delimiterList[strItr];
              short flag = 0;
              for (int charItr = 0; charItr < len; charItr++)
              {
                if (len != charItr + 1) {
                  flag = 64;
                } else {
                  flag = 32;
                }
                newHash[delimiter.charAt(charItr)] = flag;
              }
            }
          }
        }
      }
      this.eventToHashMap.put(eventString, newHash);
      hash = newHash;
    }
    return hash;
  }
  
  protected void initializeInternalDelimiters()
  {
    if (this.specialDelimiters == null)
    {
      this.specialDelimiters = new HashMap();
      List<String> list = new ArrayList();
      this.otherChars = "";
      for (short itr = 0; itr < 13; itr = (short)(itr + 1))
      {
        list.clear();
        String additionalChars = additionalChars(itr);
        this.otherChars += additionalChars;
        
        String checkpointChars = checkpointChars(itr);
        for (int charItr = 0; charItr < checkpointChars.length(); charItr++) {
          if (charItr == 0) {
            list.add(additionalChars + checkpointChars.charAt(charItr));
          } else {
            list.add("" + checkpointChars.charAt(charItr));
          }
        }
        this.specialDelimiters.put(Integer.valueOf(itr), list.toArray(new String[list.size()]));
      }
    }
  }
  
  protected void initializeCharToCodeMap()
  {
    initializeCharToCodeMap(this.colDelimiter, this.charToCodeMap, this.codeToCharMap);
    initializeCharToCodeMap(this.rowDelimiter, this.charToCodeMap, this.codeToCharMap);
    if (this.prop.quoteSetList != null) {
      initializeCharToCodeMap(this.prop.quoteSetList, this.charToCodeMap, this.codeToCharMap);
    }
    int cnt = noOfSpecialChars();
    if (cnt > 0)
    {
      List<String> list = new ArrayList();
      this.specialCharHash = new short[cnt];
      this.specialChars = new short[cnt];
      for (int itr = 0; itr < cnt; itr++)
      {
        this.specialChars[itr] = ((short)(255 - cnt + itr));
        list.add("" + (char)(255 - cnt + itr));
      }
      if (this.specialDelimiters != null)
      {
        String[] tmp = (String[])this.specialDelimiters.get(Integer.valueOf(1));
        for (int itr = 0; itr < tmp.length; itr++) {
          list.add(tmp[itr]);
        }
        this.specialDelimiters.put(Integer.valueOf(1), list.toArray(new String[list.size()]));
      }
    }
    if (this.specialDelimiters != null) {
      for (Map.Entry<Integer, String[]> entry : this.specialDelimiters.entrySet()) {
        initializeCharToCodeMap((String[])entry.getValue(), this.charToCodeMap, this.codeToCharMap);
      }
    }
  }
  
  public void initLookupTable()
  {
    try
    {
      this.hash = new short[65536];
      this.charToCodeMap = new short[65536];
      this.codeToCharMap = new char[65536];
      
      this.maxPossibleCombinationLength = maxPossibleDelimiterLength();
      
      initializeCharToCodeMap();
      this.bitCnt = getNumberOfBits(this.lastCharValue);
      initializeBitMask();
      
      validateProperty();
      
      initEventHash(this.colDelimiter, this.hash, (short)1);
      initEventHash(this.rowDelimiter, this.hash, (short)2);
      if (this.prop.quoteSetList != null) {
        initEventHash(this.prop.quoteSetList, this.hash, (short)3);
      }
      for (int eventItr = 0; eventItr < 13; eventItr++)
      {
        String[] tmp = getDelimiters((short)eventItr);
        if (tmp != null) {
          initEventHash(tmp, this.hash, (short)eventItr);
        }
      }
      if (this.specialCharHash != null) {
        for (int charItr = 0; charItr < this.specialCharHash.length; charItr++) {
          this.charToCodeMap[this.specialCharHash[charItr]] = 0;
        }
      }
      this.eventToDelimiter = new HashMap();
      this.eventToDelimiter.put(Integer.valueOf(1), this.prop.columnDelimiterList);
      this.eventToDelimiter.put(Integer.valueOf(2), this.prop.rowDelimiterList);
      this.eventToDelimiter.put(Integer.valueOf(3), this.prop.quoteSetList);
      
      this.defaultDelimiterToEventMap = new HashMap();
      this.defaultDelimiterToEventMap.putAll(this.delimiterToEventMap);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
  
  private void initializeCharToCodeMap(String[] delimiter, short[] codeMap, char[] charMap)
  {
    for (int itr = 0; itr < delimiter.length; itr++) {
      for (int charItr = 0; charItr < delimiter[itr].length(); charItr++)
      {
        int charIndex = delimiter[itr].charAt(charItr);
        if (codeMap[charIndex] == 0)
        {
          codeMap[charIndex] = (this.lastCharValue = (short)(this.lastCharValue + 1));
          charMap[this.lastCharValue] = delimiter[itr].charAt(charItr);
        }
      }
    }
  }
  
  private int getNumberOfBits(int value)
  {
    for (int bitCnt = 0; value > 0; bitCnt++) {
      value >>= 1;
    }
    return bitCnt;
  }
  
  protected String[] getDelimiters(short eventId)
  {
    return new String[0];
  }
  
  protected String additionalChars(short eventId)
  {
    return "";
  }
  
  protected String checkpointChars(short eventId)
  {
    return "";
  }
  
  protected int noOfSpecialChars()
  {
    return 0;
  }
  
  protected void initializeBitMask()
  {
    int bitCnt = getNumberOfBits(this.lastCharValue);
    int cnt = bitCnt;
    int mask = 1;
    this.bitMask = 0L;
    while (bitCnt - 1 > 0)
    {
      mask <<= 1;
      mask |= 0x1;
      bitCnt--;
    }
    this.charMask = mask;
    for (int idx = 0; idx < this.maxPossibleCombinationLength; idx++)
    {
      this.bitMask <<= cnt;
      this.bitMask |= mask;
    }
  }
  
  private void initEventHash(String[] delimiter, short[] hashTable, short eventType)
    throws AdapterException
  {
    int hashValue = 0;
    for (int idx = 0; idx < delimiter.length; idx++)
    {
      short flag = 0;
      if ((!delimiter[idx].isEmpty()) && (!checkForRegEx(delimiter[idx])))
      {
        if (delimiter[idx].length() > 1)
        {
          for (int itr = 0; itr < delimiter[idx].length(); itr++)
          {
            if (hashTable[delimiter[idx].charAt(itr)] != 0) {
              flag = hashTable[delimiter[idx].charAt(itr)];
            } else {
              flag = 0;
            }
            if (itr == 0)
            {
              flag = (short)(flag | 0x10);
            }
            else if (itr == delimiter[idx].length() - 1)
            {
              flag = (short)(flag | 0x20);
              flag = (short)(flag | eventType);
            }
            else if (flag == 0)
            {
              flag = 64;
            }
            hashTable[((short)delimiter[idx].charAt(itr))] = flag;
            hashValue <<= this.bitCnt;
            hashValue += this.charToCodeMap[((short)delimiter[idx].charAt(itr))];
          }
        }
        else
        {
          flag = (short)(flag | 0x20);
          flag = (short)(flag | eventType);
          hashValue = this.charToCodeMap[((short)delimiter[idx].charAt(0))];
          hashTable[((short)delimiter[idx].charAt(0))] = flag;
        }
        if (this.eventMap == null)
        {
          this.eventMap = new TreeMap();
          this.delimiterToEventMap = new HashMap();
        }
        SMEvent eventData = EventFactory.createEvent(eventType);
        if (eventType == 2) {
          ((RowEvent)eventData).array(this.stateMachine.buffer());
        }
        eventData.length(delimiter[idx].length());
        List<Integer> hashList = computeHash(delimiter[idx]);
        for (int itr = 0; itr < hashList.size(); itr++) {
          this.eventMap.put(Long.valueOf(((Integer)hashList.get(itr)).longValue()), eventData);
        }
        hashValue = 0;
      }
      else
      {
        throw new AdapterException("Delimiter [" + delimiter[idx] + "] contains RegEx. and it is not supported.");
      }
    }
  }
  
  private List<Integer> computeHash(String delimiter)
  {
    if (this.allDelimiters == null)
    {
      this.allDelimiters = new ArrayList();
      for (int itr = 0; itr < this.colDelimiter.length; itr++) {
        this.allDelimiters.add(this.colDelimiter[itr]);
      }
      for (int itr = 0; itr < this.rowDelimiter.length; itr++) {
        this.allDelimiters.add(this.rowDelimiter[itr]);
      }
      if (this.prop.quoteSetList != null) {
        for (int itr = 0; itr < this.prop.quoteSetList.length; itr++) {
          this.allDelimiters.add(this.prop.quoteSetList[itr]);
        }
      }
      if (this.specialChars != null) {
        for (int itr = 0; itr < this.specialChars.length; itr++) {
          this.allDelimiters.add("" + (char)this.specialChars[itr]);
        }
      }
    }
    Combination combination = new Combination(this.allDelimiters, delimiter, this.otherChars);
    this.possibleCombinations = combination.getCombinationLists();
    
    List<Integer> hashList = new ArrayList();
    int hashValue = computeHashFor(delimiter);
    hashList.add(Integer.valueOf(hashValue));
    for (int combinationItr = 0; combinationItr < this.possibleCombinations.size(); combinationItr++)
    {
      String possibleDelimiter = (String)this.possibleCombinations.get(combinationItr) + delimiter;
      hashValue = computeHashFor(possibleDelimiter);
      hashList.add(Integer.valueOf(hashValue));
    }
    return hashList;
  }
  
  public int computeHashFor(String delimiter)
  {
    int hashValue = 0;
    for (int charItr = 0; charItr < delimiter.length(); charItr++)
    {
      hashValue <<= this.bitCnt;
      hashValue += this.charToCodeMap[((short)delimiter.charAt(charItr))];
    }
    hashValue = (int)(hashValue & this.bitMask);
    return hashValue;
  }
  
  private boolean checkForRegEx(String delimiter)
  {
    return false;
  }
  
  private boolean evaluateChar(short charIndex)
  {
    if (this.hash[charIndex] != 0)
    {
      this.eventType = this.hash[charIndex];
      if ((this.eventType & 0x20) != 0)
      {
        this.hashValue <<= this.bitCnt;
        this.hashValue += this.charToCodeMap[charIndex];
        
        this.hashValue &= this.bitMask;
        
        this.hashValue = compressHash(this.hashValue);
        
        SMEvent eventData = lookForMatchingDelimiter(this.hashValue);
        if (eventData != null)
        {
          this.stateMachine.publishEvent(eventData);
          if (this.stateMachine.canBreak()) {
            return true;
          }
        }
      }
      else
      {
        this.hashValue <<= this.bitCnt;
        this.hashValue += this.charToCodeMap[charIndex];
      }
    }
    else
    {
      this.hashValue = 0L;
    }
    return false;
  }
  
  protected SMEvent lookForMatchingDelimiter(long hashValue)
  {
    return (SMEvent)this.eventMap.get(Long.valueOf(hashValue));
  }
  
  public long compressHash(long hashValue2)
  {
    return hashValue2;
  }
  
  public void reset()
  {
    this.hashValue = 0L;
  }
  
  public void next()
    throws RecordException, AdapterException
  {
    this.hashValue = 0L;
    short charIndex;
    do
    {
      charIndex = (short)this.stateMachine.getChar();
    } while (evaluateChar(charIndex) != true);
  }
  
  public boolean hasNext()
  {
    return false;
  }
  
  public void ignoreEvents(short[] eventTypes)
  {
    this.hash = createHash(eventTypes);
    assert (this.hash != null);
  }
  
  public void ignoreEvents(short[] eventTypes, SMEvent event)
  {
    ignoreEvents(eventTypes);
  }
  
  public void initializeDelimiter(String[] delimters, short eventType) {}
  
  public void initializeDelimiters() {}
  
  public void finalizeDelimiterInit() {}
  
  private static class Combination
  {
    private List<String> combinations;
    private int maxPossibleCombinationLength;
    private int delimiterLength;
    private String otherChars;
    
    public Combination(List<String> delimiters, String delimiter, String otherChars)
    {
      this.otherChars = otherChars;
      this.combinations = new ArrayList();
      this.delimiterLength = delimiter.length();
      listCombinations(delimiters);
    }
    
    public void listCombinations(List<String> list)
    {
      StringBuilder combinedStringBuilder = new StringBuilder();
      int maxLen = 0;
      for (int itr = 0; itr < list.size(); itr++)
      {
        combinedStringBuilder.append((String)list.get(itr));
        if (((String)list.get(itr)).length() > maxLen) {
          maxLen = ((String)list.get(itr)).length();
        }
      }
      this.maxPossibleCombinationLength = (maxLen * 2 - 1);
      
      combinedStringBuilder.append(this.otherChars);
      String combinedString = combinedStringBuilder.toString();
      for (int itr = 1; itr <= this.maxPossibleCombinationLength - this.delimiterLength; itr++) {
        createCom(combinedString, "", itr);
      }
    }
    
    public void createCom(String orig, String part, int iteration)
    {
      if (iteration - 1 > 0)
      {
        iteration -= 1;
        for (int i = 0; i < orig.length(); i++) {
          createCom(orig, part + orig.charAt(i), iteration);
        }
      }
      else
      {
        for (int i = 0; i < orig.length(); i++)
        {
          String tmp = part + orig.charAt(i);
          this.combinations.add(tmp);
        }
      }
    }
    
    public List<String> getCombinationLists()
    {
      return this.combinations;
    }
    
    public int maxPossibleCombinationLength()
    {
      return this.maxPossibleCombinationLength;
    }
  }
}
