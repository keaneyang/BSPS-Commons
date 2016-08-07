package com.bloom.source.lib.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

class MapFieldModifier
  extends FieldModifier
{
  private boolean formatAsJson = false;
  private String emptyJsonObjectString = "";
  private String openCurlyBrace = "";
  private String closeCurlyBrace = "";
  private String quote = "";
  private String delimiter = ",";
  
  MapFieldModifier(Map<String, Object> modifierProperties)
  {
    String formatterClassName = (String)modifierProperties.get("handler");
    String delimiter = (String)modifierProperties.get("columndelimiter");
    if ((delimiter != null) && (!delimiter.trim().isEmpty())) {
      this.delimiter = delimiter;
    }
    if (formatterClassName.toLowerCase().contains("json"))
    {
      this.formatAsJson = true;
      this.emptyJsonObjectString = "{}";
      this.openCurlyBrace = "{";
      this.closeCurlyBrace = "}";
      this.quote = "\"";
    }
  }
  
  public String modifyFieldValue(Object fieldValue, Object event)
  {
    Map<?, ?> metadataMap = (Map)fieldValue;
    Iterator<?> metadataMapIterator = metadataMap.entrySet().iterator();
    if (!metadataMapIterator.hasNext()) {
      return this.emptyJsonObjectString;
    }
    StringBuilder metadataBuilder = new StringBuilder();
    metadataBuilder.append(this.openCurlyBrace);
    for (;;)
    {
      Map.Entry<?, ?> e = (Map.Entry)metadataMapIterator.next();
      if (e != null)
      {
        String key = e.getKey().toString();
        Object value = e.getValue();
        if (this.formatAsJson)
        {
          metadataBuilder.append(this.quote + key + this.quote + ":");
          if (value == null) {
            value = "null";
          } else {
            value = this.quote + value + this.quote;
          }
        }
        metadataBuilder.append(value);
        if (!metadataMapIterator.hasNext()) {
          return this.closeCurlyBrace;
        }
        metadataBuilder.append(this.delimiter);
      }
    }
  }
}
