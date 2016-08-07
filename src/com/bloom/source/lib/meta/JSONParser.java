package com.bloom.source.lib.meta;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.constant.Constant.fieldType;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.event.ObjectMapperFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;

public class JSONParser
{
  String metaInput = null;
  JsonNode node = null;
  JsonNode fields = null;
  Column column = null;
  Map<String, Column[]> colMap;
  
  public JSONParser(String metaDatafilename)
    throws AdapterException
  {
    this.colMap = new HashMap();
    try
    {
      FileInputStream jsonFile = new FileInputStream(metaDatafilename);
      ObjectMapper mapper = ObjectMapperFactory.newInstance();
      this.node = mapper.readTree(jsonFile);
      loadColMap(null, this.node);
      setJSONMetaFields();
      jsonFile.close();
    }
    catch (IOException ioExp)
    {
      throw new AdapterException(Error.GENERIC_EXCEPTION, ioExp);
    }
  }
  
  public Column[] getColumns(String path)
  {
    return (Column[])this.colMap.get(path);
  }
  
  private Column createColumnObject(JsonNode node)
    throws AdapterException
  {
    if ((node.isContainerNode()) && (!node.isArray()))
    {
      String name = node.get("name").textValue();
      String dataType = node.get("type").textValue();
      int position = node.get("position").intValue();
      
      int size = -1;
      if (node.get("size") != null) {
        size = node.get("size").intValue();
      }
      Column column;
      if (dataType.equals("INTEGER"))
      {
        column = new IntegerColumn();
      }
      else
      {
        if (dataType.equals("DOUBLE"))
        {
          column = new DoubleColumn();
        }
        else
        {
          if (dataType.equals("FLOAT"))
          {
            column = new FloatColumn();
          }
          else
          {
            if (dataType.equals("LONG"))
            {
              column = new LongColumn();
            }
            else
            {
              if (dataType.equals("SHORT"))
              {
                column = new ShortColumn();
              }
              else
              {
                if (dataType.equals("STRING"))
                {
                  column = new StringColumn();
                }
                else
                {
                  if (dataType.equals("BYTE"))
                  {
                    column = new ByteColumn();
                  }
                  else
                  {
                    String errmsg = "Column type [" + dataType + "] is not supported";
                    throw new AdapterException(Error.UNSUPORTED_DATATYPE, errmsg);
                  }
                }
              }
            }
          }
        }
      }
      if (size != -1) {
        column.setSize(size);
      }
      column.setName(name);
      column.setIndex(position);
      return column;
    }
    throw new AdapterException(Error.INVALID_JSON_NODE);
  }
  
  private void loadColMap(String path, JsonNode node)
    throws AdapterException
  {
    Iterator<String> fieldNames = node.fieldNames();
    if (fieldNames.hasNext()) {
      while (fieldNames.hasNext())
      {
        String field = (String)fieldNames.next();
        if (field == "Ver")
        {
          String ver = new Integer(node.get(field).intValue()).toString();
          if (path != null) {
            path = path + "." + ver;
          } else {
            path = ver;
          }
        }
        else if (path != null)
        {
          path = path + "." + field;
        }
        else
        {
          path = field;
        }
        if (field != "fields")
        {
          JsonNode childNode = node.get(field);
          loadColMap(path, childNode);
        }
        else if ((node.isContainerNode() == true) && (!node.isArray()))
        {
          JsonNode childNode = node.get(field);
          int size = childNode.size();
          if (size != 0)
          {
            Column[] fields = new Column[childNode.size()];
            for (int idx = 0; idx < childNode.size(); idx++) {
              fields[idx] = createColumnObject(childNode.get(idx));
            }
            this.colMap.put(path, fields);
          }
          else
          {
            System.out.println("No field definition found for [" + path + "]");
          }
        }
      }
    }
    if (node.isArray()) {
      for (int idx = 0; idx < node.size(); idx++)
      {
        JsonNode childNode = node.get(idx);
        loadColMap(path, childNode);
      }
    }
  }
  
  public void setJSONMetaFields()
    throws JsonProcessingException, IOException
  {
    this.fields = getJSONMetaFields();
  }
  
  public JsonNode getJSONMetaFields()
    throws JsonProcessingException, IOException
  {
    this.fields = this.node.get("fields");
    return this.fields;
  }
  
  public int getFieldCount()
  {
    return this.fields.size();
  }
  
  public Column fillColumnMetaData(Hashtable<Integer, Column> columnMetaData, int endian, boolean nullTerminatedString, int colIndex)
  {
    String dataType = null;
    String name = null;
    int index = 0;
    int size = 0;
    if (this.fields != null)
    {
      this.node = this.fields.get(colIndex);
      if (this.node.has("name")) {
        name = this.node.get("name").textValue();
      }
      if (this.node.has("type")) {
        dataType = this.node.get("type").textValue();
      }
      if (this.node.has("position")) {
        index = this.node.get("position").intValue();
      }
      if (this.node.has("size")) {
        size = this.node.get("size").intValue();
      }
      if (endian == 1)
      {
        if (dataType.equals("INTEGER"))
        {
          this.column = new IntegerColumn();
          this.column.setType(Constant.fieldType.INTEGER);
          this.column.setSize(Constant.INTEGER_SIZE);
        }
        else if (dataType.equals("DOUBLE"))
        {
          this.column = new DoubleColumn();
          this.column.setType(Constant.fieldType.DOUBLE);
          this.column.setSize(Constant.DOUBLE_SIZE);
        }
        else if (dataType.equals("FLOAT"))
        {
          this.column = new FloatColumn();
          this.column.setType(Constant.fieldType.FLOAT);
          this.column.setSize(Constant.INTEGER_SIZE);
        }
        else if (dataType.equals("LONG"))
        {
          this.column = new LongColumn();
          this.column.setType(Constant.fieldType.LONG);
          this.column.setSize(Constant.DOUBLE_SIZE);
        }
        else if (dataType.equals("SHORT"))
        {
          this.column = new ShortColumn();
          this.column.setType(Constant.fieldType.SHORT);
          this.column.setSize(Constant.SHORT_SIZE);
        }
        else if ((!nullTerminatedString) && 
          (dataType.equals("STRING")))
        {
          this.column = new StringColumn();
          this.column.setType(Constant.fieldType.STRING);
        }
      }
      else if (dataType.equals("INTEGER"))
      {
        this.column = new LEIntegerColumn();
        this.column.setType(Constant.fieldType.INTEGER);
        this.column.setSize(Constant.INTEGER_SIZE);
      }
      else if (dataType.equals("DOUBLE"))
      {
        this.column = new LEDoubleColumn();
        this.column.setType(Constant.fieldType.DOUBLE);
        this.column.setSize(Constant.DOUBLE_SIZE);
      }
      else if (dataType.equals("FLOAT"))
      {
        this.column = new LEFloatColumn();
        this.column.setType(Constant.fieldType.FLOAT);
        this.column.setSize(Constant.INTEGER_SIZE);
      }
      else if (dataType.equals("LONG"))
      {
        this.column = new LELongColumn();
        this.column.setType(Constant.fieldType.LONG);
        this.column.setSize(Constant.DOUBLE_SIZE);
      }
      else if (dataType.equals("SHORT"))
      {
        this.column = new ShortColumn();
        this.column.setType(Constant.fieldType.SHORT);
        this.column.setSize(Constant.SHORT_SIZE);
      }
      else if ((!nullTerminatedString) && 
        (dataType.equals("STRING")))
      {
        this.column = new LEStringColumn();
        this.column.setType(Constant.fieldType.STRING);
        this.column.setSize(size);
      }
      if (dataType.equals("BYTE"))
      {
        this.column = new ByteColumn();
        this.column.setType(Constant.fieldType.BYTE);
        this.column.setSize(Constant.BYTE_SIZE);
      }
      if ((nullTerminatedString) && 
        (dataType.equals("STRING")))
      {
        this.column = new StringColumnNullTerminated();
        this.column.setType(Constant.fieldType.STRING);
      }
      this.column.setIndex(index);
      this.column.setName(name);
      columnMetaData.put(Integer.valueOf(colIndex), this.column);
    }
    return this.column;
  }
}
