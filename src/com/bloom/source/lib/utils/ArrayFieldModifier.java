package com.bloom.source.lib.utils;

import java.lang.reflect.Field;
import java.util.Map;

import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;

class ArrayFieldModifier
  extends FieldModifier
{
  private BaseArray typedArray;
  private boolean formatAsJson = false;
  private String nullValue;
  private String emptyJsonObjectString = "";
  private String openCurlyBrace = "";
  private String closeCurlyBrace = "";
  private String quote = "";
  private String delimiter = ",";
  
  ArrayFieldModifier(Map modifierProperties)
  {
    Property prop = new Property(modifierProperties);
    String formatterClassName = (String)modifierProperties.get("handler");
    this.nullValue = prop.getString("nullvalue", "NULL");
    String delimiter = (String)modifierProperties.get("columndelimiter");
    if ((delimiter != null) && (!delimiter.trim().isEmpty())) {
      this.delimiter = delimiter;
    }
    if (formatterClassName.toLowerCase().contains("json"))
    {
      this.formatAsJson = true;
      this.nullValue = "null";
      this.emptyJsonObjectString = "{}";
      this.openCurlyBrace = "{";
      this.closeCurlyBrace = "}";
      this.quote = "\"";
    }
  }
  
  public void setTypedArray(BaseArray typedArray)
  {
    this.typedArray = typedArray;
  }
  
  public String modifyFieldValue(Object fieldValue, Object event)
  {
    return this.typedArray.toString(fieldValue);
  }
  
  static abstract class BaseArray
  {
    static BaseArray getTypedArray(Field field, ArrayFieldModifier arrayFieldModifier)
      throws AdapterException
    {
      if (field.getType() == Object[].class)
      {
        ArrayFieldModifier tmp15_14 = arrayFieldModifier;tmp15_14.getClass();return new ArrayFieldModifier.ObjectArray(tmp15_14);
      }
      if (field.getType() == int[].class)
      {
        ArrayFieldModifier tmp39_38 = arrayFieldModifier;tmp39_38.getClass();return new ArrayFieldModifier.IntegerArray(tmp39_38);
      }
      if (field.getType() == boolean[].class)
      {
        ArrayFieldModifier tmp63_62 = arrayFieldModifier;tmp63_62.getClass();return new ArrayFieldModifier.BooleanArray(tmp63_62);
      }
      if (field.getType() == float[].class)
      {
        ArrayFieldModifier tmp87_86 = arrayFieldModifier;tmp87_86.getClass();return new ArrayFieldModifier.FloatArray(tmp87_86);
      }
      if (field.getType() == double[].class)
      {
        ArrayFieldModifier tmp111_110 = arrayFieldModifier;tmp111_110.getClass();return new ArrayFieldModifier.DoubleArray(tmp111_110);
      }
      if (field.getType() == long[].class)
      {
        ArrayFieldModifier tmp135_134 = arrayFieldModifier;tmp135_134.getClass();return new ArrayFieldModifier.LongArray(tmp135_134);
      }
      if (field.getType() == char[].class)
      {
        ArrayFieldModifier tmp159_158 = arrayFieldModifier;tmp159_158.getClass();return new ArrayFieldModifier.CharacterArray(tmp159_158);
      }
      if (field.getType() == byte[].class)
      {
        ArrayFieldModifier tmp183_182 = arrayFieldModifier;tmp183_182.getClass();return new ArrayFieldModifier.ByteArray(tmp183_182);
      }
      if (field.getType() == short[].class)
      {
        ArrayFieldModifier tmp207_206 = arrayFieldModifier;tmp207_206.getClass();return new ArrayFieldModifier.ShortArray(tmp207_206);
      }
      throw new AdapterException("Failure in writer initialization. Unsupported array type " + field.getType().getCanonicalName());
    }
    
    abstract String toString(Object paramObject);
  }
  
  class IntegerArray
    extends ArrayFieldModifier.BaseArray
  {
    IntegerArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      int[] dataArray = (int[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class BooleanArray
    extends ArrayFieldModifier.BaseArray
  {
    BooleanArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      boolean[] dataArray = (boolean[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class FloatArray
    extends ArrayFieldModifier.BaseArray
  {
    FloatArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      float[] dataArray = (float[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class DoubleArray
    extends ArrayFieldModifier.BaseArray
  {
    DoubleArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      double[] dataArray = (double[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
      }
    }
  }
  
  class ShortArray
    extends ArrayFieldModifier.BaseArray
  {
    ShortArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      short[] dataArray = (short[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class LongArray
    extends ArrayFieldModifier.BaseArray
  {
    LongArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      long[] dataArray = (long[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class ObjectArray
    extends ArrayFieldModifier.BaseArray
  {
    ObjectArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      Object[] dataArray = (Object[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        Object dataValue = dataArray[i];
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        } else {
          dataValue = ArrayFieldModifier.this.quote + dataValue + ArrayFieldModifier.this.quote;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class CharacterArray
    extends ArrayFieldModifier.BaseArray
  {
    CharacterArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      char[] dataArray = (char[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(ArrayFieldModifier.this.quote + dataValue + ArrayFieldModifier.this.quote);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
  
  class ByteArray
    extends ArrayFieldModifier.BaseArray
  {
    ByteArray(ArrayFieldModifier afm) {}
    
    String toString(Object fieldValue)
    {
      byte[] dataArray = (byte[])fieldValue;
      if (dataArray == null) {
        return ArrayFieldModifier.this.nullValue;
      }
      int lastIndex = dataArray.length - 1;
      if (lastIndex == -1) {
        return ArrayFieldModifier.this.emptyJsonObjectString;
      }
      StringBuilder b = new StringBuilder();
      b.append(ArrayFieldModifier.this.openCurlyBrace);
      for (int i = 0;; i++)
      {
        String dataValue = String.valueOf(dataArray[i]);
        if (dataValue == null) {
          dataValue = ArrayFieldModifier.this.nullValue;
        }
        if (ArrayFieldModifier.this.formatAsJson) {
          b.append(ArrayFieldModifier.this.quote + i + ArrayFieldModifier.this.quote + ":");
        }
        b.append(dataValue);
        if (i == lastIndex) {
          return ArrayFieldModifier.this.closeCurlyBrace;
        }
        b.append(ArrayFieldModifier.this.delimiter);
      }
    }
  }
}
