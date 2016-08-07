package com.bloom.source.lib.rs;

import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.nvp.NVPProperty;
import com.bloom.source.nvp.NameValueParser;
import com.bloom.common.constants.Constant;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

public abstract class CharResultSet
  extends ResultSet
{
  protected char[] rowData;
  Logger logger = Logger.getLogger(ResultSet.class);
  
  public CharResultSet(Reader reader, Property prop)
    throws IOException, InterruptedException
  {
    super(reader, prop);
  }
  
  protected void Init()
    throws IOException, InterruptedException
  {
    this.rowData = new char[this.reader.blockSize()];
    super.Init();
  }
  
  public String getColumnValue(int columnIndex)
  {
    if ((this.columnLength[columnIndex] != 0) && (this.columnOffset[columnIndex] != -1))
    {
      String value = new String(this.rowData, this.columnOffset[columnIndex], this.columnLength[columnIndex]);
      if (this.prop.trimwhitespace) {
        return value.trim();
      }
      return value;
    }
    return "";
  }
  
  public char[] getRowData()
  {
    return this.rowData;
  }
  
  public void setRowData(char[] rowData)
  {
    this.rowData = rowData;
  }
  
  public Constant.recordstatus next()
    throws IOException, InterruptedException
  {
    return null;
  }
  
  public Map<String, String> getColumnValueAsMap(int index)
    throws AdapterException
  {
    return getColumnValueAsMap(getColumnValue(index));
  }
  
  public Map<String, String> getColumnValueAsMap(String text)
    throws AdapterException
  {
    NameValueParser nvp = new NameValueParser(new NVPProperty(this.prop.getMap()));
    String columnValue = text;
    if ((columnValue.charAt(0) == this.prop.quotecharacter) && (columnValue.charAt(columnValue.length() - 1) == this.prop.quotecharacter))
    {
      columnValue = columnValue.substring(1, columnValue.length() - 1);
      return nvp.convertToMap(columnValue);
    }
    return nvp.convertToMap(columnValue);
  }
  
  public List<String> applyRegexOnColumnValue(String columnValue, String regex)
  {
    return null;
  }
}
