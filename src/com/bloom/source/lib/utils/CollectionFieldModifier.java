package com.bloom.source.lib.utils;

class CollectionFieldModifier
  extends FieldModifier
{
  private String collectionDelimiter = ",";
  
  CollectionFieldModifier(String collectionDelimiter)
  {
    if ((collectionDelimiter != null) && (!collectionDelimiter.trim().isEmpty())) {
      this.collectionDelimiter = collectionDelimiter;
    }
  }
  
  public String modifyFieldValue(Object fieldValue, Object event)
  {
    String value = fieldValue.toString();
    
    value = value.replaceAll("\\[", "");
    value = value.replaceAll("\\]", "");
    if (!this.collectionDelimiter.equals(",")) {
      value = value.replaceAll(",", this.collectionDelimiter);
    }
    return value;
  }
}
