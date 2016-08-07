package com.bloom.source.smlite;

import java.util.HashMap;
import java.util.Map;

public class PatternFactory
{
  public static final String DEFAULT_PATTERN = "Pattern";
  public static final String DATE_PATTERN = "DatePattern";
  public static final String IP_PATTERN = "IPPattern";
  static Map<String, String> typeMap = new HashMap() {};
  static Map<String, Pattern> patternMap = new HashMap() {};
  
  static synchronized Pattern createPattern(String patternString)
  {
    String type = getPatternType(patternString);
    Pattern pattern = (Pattern)patternMap.get(type + patternString);
    if (pattern == null)
    {
      pattern = (Pattern)patternMap.get(type);
      pattern = (Pattern)pattern.clone();
      if (pattern == null) {
        return pattern;
      }
      pattern.init(patternString);
      patternMap.put(type + patternString, pattern);
    }
    return pattern;
  }
  
  static String getPatternType(String patternString)
  {
    String pattern = "Pattern";
    if (patternString.length() > 1)
    {
      int startIdx = patternString.indexOf('%');
      if (startIdx != -1)
      {
        int endIndex = patternString.indexOf('%', startIdx + 1);
        if (endIndex != -1) {
          if ((endIndex - startIdx > 2) && (patternString.indexOf('%', endIndex + 1) == -1))
          {
            String tmpType = patternString.substring(startIdx + 1, endIndex);
            tmpType = (String)typeMap.get(tmpType);
            if (tmpType != null) {
              pattern = tmpType;
            }
          }
          else
          {
            pattern = "DatePattern";
          }
        }
      }
    }
    return pattern;
  }
}
