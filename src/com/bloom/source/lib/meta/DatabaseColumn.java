package com.bloom.source.lib.meta;

import com.bloom.source.lib.type.columntype;

import org.apache.log4j.Logger;

public abstract class DatabaseColumn
  extends Column
{
  public String dataTypeName = null;
  int dataLength = 0;
  int precision = 0;
  int scale = 0;
  public String nullable = null;
  boolean isKey = false;
  public int typeCode = 0;
  boolean isExcluded = false;
  private static Logger logger = Logger.getLogger(DatabaseColumn.class);
  public columntype internalType;
  
  public String getDataTypeName()
  {
    return this.dataTypeName;
  }
  
  public int getDataLength()
  {
    return this.dataLength;
  }
  
  public int getPrecision()
  {
    return this.precision;
  }
  
  public int getScale()
  {
    return this.scale;
  }
  
  public boolean isNullable()
  {
    return true;
  }
  
  public boolean isKey()
  {
    return this.isKey;
  }
  
  public boolean isExcluded()
  {
    return this.isExcluded;
  }
  
  public void setIndex(int index)
  {
    this.index = index;
  }
  
  public void setName(String name)
  {
    this.name = name;
  }
  
  public void setDataTypeName(String string)
  {
    this.dataTypeName = string;
    switch (this.dataTypeName)
    {
    case "VARCHAR2": 
      this.typeCode = 1;
      break;
    case "NUMBER": 
      this.typeCode = 2;
      break;
    case "NVARCHAR2": 
      this.typeCode = 3;
      break;
    case "LONG": 
      this.typeCode = 8;
      break;
    case "DATE": 
      this.typeCode = 12;
      break;
    case "RAW": 
      this.typeCode = 23;
      break;
    case "LONG RAW": 
      this.typeCode = 24;
      break;
    case "ROWID": 
      this.typeCode = 69;
      break;
    case "CHAR": 
      this.typeCode = 96;
      break;
    case "NCHAR": 
      this.typeCode = 97;
      break;
    case "BINARY_FLOAT": 
      this.typeCode = 100;
      break;
    case "BINARY_DOUBLE": 
      this.typeCode = 101;
      break;
    case "NCLOB": 
      this.typeCode = 111;
      break;
    case "CLOB": 
      this.typeCode = 112;
      break;
    case "BLOB": 
      this.typeCode = 113;
      break;
    case "BFILE": 
      this.typeCode = 114;
      break;
    case "TIMESTAMP(6)": 
      this.typeCode = 180;
      break;
    case "TIMESTAMP(6) WITH TIME ZONE": 
      this.typeCode = 181;
      break;
    case "INTERVAL YEAR TO MONTH": 
      this.typeCode = 182;
      break;
    case "INTERVAL DAY TO SECOND": 
      this.typeCode = 183;
      break;
    case "UROWID": 
      this.typeCode = 208;
      break;
    case "TIMESTAMP(6) WITH LOCAL TIME ZONE": 
      this.typeCode = 231;
    }
  }
  
  public void setDataLength(int dataLength)
  {
    this.dataLength = dataLength;
  }
  
  public void setPrecision(int precision)
  {
    this.precision = precision;
  }
  
  public void setScale(int scale)
  {
    this.scale = scale;
  }
  
  public void setNullable(String string)
  {
    this.nullable = string;
  }
  
  public void setKey(boolean isKey)
  {
    this.isKey = isKey;
  }
  
  public void setExcluded(boolean isExcluded)
  {
    this.isExcluded = isExcluded;
  }
  
  public int getTypeCode()
  {
    return this.typeCode;
  }
  
  public void setTypeCode(int typeCode)
  {
    this.typeCode = typeCode;
  }
  
  public columntype getInternalColumnType()
  {
    return this.internalType;
  }
  
  public abstract void setInternalColumnType(String paramString);
  
  public static DatabaseColumn initializeDataBaseColumnFromProductName(String dbProductName)
  {
    if (dbProductName == null) {
      return null;
    }
    String nameToCheck = dbProductName.toLowerCase();
    if (nameToCheck.indexOf("oracle") != -1) {
      return new OracleColumn();
    }
    if (nameToCheck.indexOf("mysql") != -1) {
      return new MySQLColumn();
    }
    if (nameToCheck.indexOf("sql server") != -1) {
      return new MSSqlColumn();
    }
    if (nameToCheck.indexOf("sql/mx") != -1) {
      return null;
    }
    logger.warn("Database Product Name is not known: " + dbProductName);
    
    return null;
  }
}
