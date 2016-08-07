package com.bloom.source.lib.formatter;

import com.bloom.proc.events.WAEvent;
import com.bloom.runtime.BuiltInFunc;

import java.lang.reflect.Field;

public class DSVCDCArrayFormatter
  extends CDCArrayFormatter
{
  public DSVCDCArrayFormatter(Field field)
  {
    super(field);
  }
  
  public String formatCDCArray(WAEvent waEvent, Object[] dataOrBeforeArray, Field[] fields)
  {
    int lastIndex = dataOrBeforeArray.length - 1;
    if (lastIndex == -1) {
      return "";
    }
    StringBuilder b = new StringBuilder();
    for (int i = 0; i <= lastIndex; i++)
    {
      if (fields != null)
      {
        boolean isPresent = BuiltInFunc.IS_PRESENT(waEvent, dataOrBeforeArray, i);
        if (isPresent) {
          appendValue(dataOrBeforeArray[i], b);
        } else {
          b.append("");
        }
      }
      else
      {
        appendValue(dataOrBeforeArray[i], b);
      }
      if (i == lastIndex) {
        break;
      }
      b.append(",");
    }
    return b.toString();
  }
  
  private void appendValue(Object value, StringBuilder b)
  {
    if (value == null) {
      value = "null";
    }
    b.append(value);
  }
}
