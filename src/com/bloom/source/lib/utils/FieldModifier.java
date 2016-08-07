package com.bloom.source.lib.utils;

import com.bloom.source.lib.formatter.DSVCDCArrayFormatter;
import com.bloom.source.lib.formatter.JSONCDCArrayFormatter;
import com.bloom.common.exc.AdapterException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Map;

public abstract class FieldModifier
{
  public static FieldModifier[] getFieldModifiers(Map<String, Object> properties, Field[] fields, String formatterType)
    throws AdapterException
  {
    FieldModifier[] fieldModifier = new FieldModifier[fields.length];
    int i = 0;
    for (Field field : fields)
    {
      if (field.getType() == String.class)
      {
        fieldModifier[i] = new StringFieldModifier(properties);
      }
      else if (Collection.class.isAssignableFrom(field.getType()))
      {
        String collectionDelimiter = (String)properties.get("columndelimiter");
        fieldModifier[i] = new CollectionFieldModifier(collectionDelimiter);
      }
      else if (Map.class.isAssignableFrom(field.getType()))
      {
        fieldModifier[i] = new MapFieldModifier(properties);
      }
      else if (field.getType().isArray())
      {
        if (field.getDeclaringClass().getSimpleName().equalsIgnoreCase("waevent"))
        {
          if (formatterType.equalsIgnoreCase("dsv")) {
            fieldModifier[i] = new DSVCDCArrayFormatter(field);
          } else if (formatterType.equalsIgnoreCase("json")) {
            fieldModifier[i] = new JSONCDCArrayFormatter(field);
          } else {
            throw new AdapterException("Formatting WAEvent data field value for " + formatterType + "formatter is not supported");
          }
        }
        else
        {
          ArrayFieldModifier arrayFieldModifier = new ArrayFieldModifier(properties);
          arrayFieldModifier.setTypedArray(ArrayFieldModifier.BaseArray.getTypedArray(field, arrayFieldModifier));
          fieldModifier[i] = arrayFieldModifier;
        }
      }
      else
      {
        fieldModifier[i] = new DefaultFieldModifier();
      }
      i++;
    }
    return fieldModifier;
  }
  
  public abstract String modifyFieldValue(Object paramObject1, Object paramObject2)
    throws Exception;
}
