package com.bloom.source.lib.utils;

import com.bloom.source.lib.prop.Property;

import java.util.Map;

class StringFieldModifier
  extends FieldModifier
{
  private boolean useQuotes;
  private String quoteCharacter;
  
  StringFieldModifier(Map<String, Object> modifierProperties)
  {
    Property prop = new Property(modifierProperties);
    this.useQuotes = prop.getBoolean("usequotes", false);
    this.quoteCharacter = prop.getString("quotecharacter", "\"");
  }
  
  public String modifyFieldValue(Object fieldValue, Object event)
  {
    String value = fieldValue.toString();
    if (this.useQuotes)
    {
      if (value.contains(this.quoteCharacter)) {
        value = value.replaceAll(this.quoteCharacter, "\\\\" + this.quoteCharacter);
      }
      return this.quoteCharacter + value + this.quoteCharacter;
    }
    return value;
  }
}
