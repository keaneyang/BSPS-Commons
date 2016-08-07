package com.bloom.source.lib.rollingpolicy.util;

import com.bloom.common.exc.AdapterException;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class RollingPolicyUtil
{
  public static Map<String, Object> mapPolicyValueToRollOverPolicyName(String uploadOrRolloverPolicy)
    throws AdapterException
  {
    Map<String, Object> parsedPolicyMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    boolean rolloverPolicySet = false;
    
    StringTokenizer tokenizer = new StringTokenizer(uploadOrRolloverPolicy, ",");
    while (tokenizer.hasMoreTokens())
    {
      String token = tokenizer.nextToken();
      String key = null;
      String value = null;
      if (token.contains(":"))
      {
        key = token.substring(0, token.indexOf(':'));
        value = token.substring(token.indexOf(':') + 1, token.length());
        if (key.equalsIgnoreCase("rotationinterval")) {
          key = "interval";
        }
      }
      else if (token.toLowerCase().contains("policy"))
      {
        key = "rolloverpolicy";
        value = token;
        rolloverPolicySet = true;
      }
      else
      {
        throw new AdapterException("Invalid Policy value " + token);
      }
      parsedPolicyMap.put(key.trim(), value.trim());
    }
    if (!rolloverPolicySet) {
      if ((parsedPolicyMap.containsKey("eventcount")) && (parsedPolicyMap.containsKey("interval"))) {
        parsedPolicyMap.put("rolloverpolicy", "EventCountAndTimeRollingPolicy");
      } else if (parsedPolicyMap.containsKey("eventcount")) {
        parsedPolicyMap.put("rolloverpolicy", "EventCountRollingPolicy");
      } else if (parsedPolicyMap.containsKey("interval")) {
        parsedPolicyMap.put("rolloverpolicy", "TimeIntervalRollingPolicy");
      } else if (parsedPolicyMap.containsKey("filesize")) {
        parsedPolicyMap.put("rolloverpolicy", "FileSizeRollingPolicy");
      } else if ((parsedPolicyMap.get("rolloverpolicy") == null) || (!((String)parsedPolicyMap.get("rolloverpolicy")).equalsIgnoreCase("DefaultRollingPolicy"))) {
        throw new IllegalArgumentException("Invalid policy value passed :" + uploadOrRolloverPolicy);
      }
    }
    return parsedPolicyMap;
  }
}
