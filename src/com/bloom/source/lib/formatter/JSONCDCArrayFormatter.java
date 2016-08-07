package com.bloom.source.lib.formatter;

import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.BuiltInFunc;

import java.lang.reflect.Field;

public class JSONCDCArrayFormatter
  extends CDCArrayFormatter
{
  public JSONCDCArrayFormatter(Field field)
  {
    super(field);
  }
  
  public String formatCDCArray(WAEvent waEvent, Object[] dataOrBeforeArray, Field[] fields)
  {
    boolean addComma = false;
    int lastIndex = dataOrBeforeArray.length - 1;
    if (lastIndex == -1) {
      return "{}";
    }
    StringBuilder b = new StringBuilder();
    b.append("{\n");
    for (int i = 0; i <= lastIndex; i++)
    {
      boolean isPresent = BuiltInFunc.IS_PRESENT(waEvent, dataOrBeforeArray, i);
      if (fields != null)
      {
        if (isPresent)
        {
          if (addComma) {
            b.append(",\n");
          }
          appendValue(fields[i].getName(), dataOrBeforeArray[i], b);
          if (!addComma) {
            addComma = true;
          }
        }
      }
      else
      {
        if (addComma) {
          b.append(",\n");
        }
        appendValue(Integer.valueOf(i), dataOrBeforeArray[i], b);
        if (!addComma) {
          addComma = true;
        }
      }
      if (i == lastIndex) {
        break;
      }
    }
    return "\n}";
  }
  
  private void appendValue(Object memberName, Object memberValue, StringBuilder b)
  {
    b.append("\"" + memberName + "\":");
    Object value = memberValue;
    if (value == null) {
      value = "null";
    } else {
      value = "\"" + value + "\"";
    }
    b.append(value);
  }
}
