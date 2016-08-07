package com.bloom.source.lib.utils;

import com.bloom.common.exc.AdapterException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;

public class XMLElementBuilder
{
  private Map<String, Object> elementMap;
  
  public XMLElementBuilder(Map<String, Object> xmlProperties, Field[] fields)
    throws AdapterException
  {
    this.elementMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    initializeMap(xmlProperties, fields);
  }
  
  private void initializeMap(Map<String, Object> xmlProperties, Field[] fields)
    throws AdapterException
  {
    String elementTuple = (String)xmlProperties.get("elementtuple");
    String[] elementTupleArray = elementTuple.split(",");
    for (String element : elementTupleArray)
    {
      Element et = new Element(element, fields);
      this.elementMap.put(et.elementName, et);
    }
  }
  
  public Map<String, Object> getElementMap()
  {
    return this.elementMap;
  }
  
  public class Element
  {
    private String elementName;
    private Map<String, Field> attributeFieldMap;
    private Field textField = null;
    
    public Element(String elementTuple, Field[] fields)
      throws AdapterException
    {
      this.attributeFieldMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
      String[] elementTupleArray = elementTuple.split(":");
      int length = elementTupleArray.length;
      this.elementName = elementTupleArray[0];
      if (length > 1)
      {
        String[] text = elementTupleArray[(length - 1)].split("=");
        if ((text.length == 2) && 
          (!text[1].trim().isEmpty())) {
          this.textField = iterateOverFields(fields, text[1]);
        }
        for (int i = 1; i < length - 1; i++)
        {
          String attributeName = elementTupleArray[i];
          this.attributeFieldMap.put(attributeName, iterateOverFields(fields, attributeName));
        }
      }
    }
    
    private Field iterateOverFields(Field[] fields, String fieldName)
      throws AdapterException
    {
      Field fieldNameToBeReturned = null;
      for (Field field : fields) {
        if (field.getName().equalsIgnoreCase(fieldName))
        {
          fieldNameToBeReturned = field;
          break;
        }
      }
      if (fieldNameToBeReturned == null) {
        throw new AdapterException("Attribute or Text dooesn't have a field/type associated with it");
      }
      return fieldNameToBeReturned;
    }
    
    public String getElementName()
    {
      return this.elementName;
    }
    
    public Map<String, Field> getAttributeFieldMap()
    {
      return this.attributeFieldMap;
    }
    
    public String processTextValue(Object event)
      throws Exception
    {
      if (this.textField != null) {
        return this.textField.get(event).toString();
      }
      return "";
    }
  }
}
