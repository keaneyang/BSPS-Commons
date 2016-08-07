package com.bloom.source.nvp;

import com.bloom.common.constants.Constant.recordstatus;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.log4j.Logger;

public class NameValueParser
{
  private final int MAX_COL_COUNT = 2;
  private final int NAME_COLUMN = 0;
  private final int VALUE_COLUMN = 1;
  private NVPResultSet resultSet;
  static Logger logger = Logger.getLogger(NameValueParser.class);
  NVPProperty prop;
  
  public NameValueParser(NVPProperty prop)
  {
    this.prop = prop;
  }
  
  public Map<String, String> convertToMap(String msg)
  {
    String additionalValue = "";
    String prevName = "";
    boolean invalidMsg = false;
    
    Map<String, String> map = new HashMap();
    try
    {
      if (this.resultSet == null) {
        this.resultSet = new NVPResultSet(this.prop);
      }
      this.resultSet.setBuffer(msg);
      while (this.resultSet.next() == recordstatus.VALID_RECORD) {
        if (this.resultSet.getColumnCount() == 2)
        {
          String value = "";
          String colName = this.resultSet.getColumnValue(0);
          String colValue = this.resultSet.getColumnValue(1);
          if (additionalValue.length() != 0) {
            if ((prevName != null) && (prevName.length() != 0))
            {
              value = (String)map.get(prevName);
              value = value + additionalValue;
              map.put(prevName, value);
              additionalValue = "";
            }
            else
            {
              additionalValue = "";
            }
          }
          colName = colName.trim();
          colValue = colValue.trim();
          map.put(colName, colValue);
          prevName = colName;
        }
        else if (this.resultSet.getColumnCount() > 2)
        {
          if (!invalidMsg)
          {
            logger.error("Error while parsing Name Value Pair for [" + msg + "]");
            invalidMsg = true;
          }
          for (int idx = 0; idx < this.resultSet.getColumnCount(); idx++)
          {
            additionalValue = additionalValue + " " + this.resultSet.getColumnValue(idx);
            additionalValue = additionalValue.trim();
          }
        }
        else if (this.resultSet.getColumnValue(0).length() != 0)
        {
          additionalValue = additionalValue + " " + this.resultSet.getColumnValue(0);
        }
      }
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
    catch (InterruptedException e1)
    {
      e1.printStackTrace();
    }
    if (additionalValue.length() != 0)
    {
      String value = (String)map.get(prevName);
      value = value + " " + additionalValue;
      map.put(prevName, value);
    }
    return map;
  }
}
