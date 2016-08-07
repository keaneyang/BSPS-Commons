package com.bloom.source.lib.utils;

class DefaultFieldModifier
  extends FieldModifier
{
  public String modifyFieldValue(Object fieldValue, Object event)
  {
    return fieldValue.toString();
  }
}
