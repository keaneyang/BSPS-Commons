package com.bloom.source.lib.rs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Observable;

import org.apache.log4j.Logger;

import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.meta.Column;
import com.bloom.source.lib.meta.MetaData;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.reader.ReaderBase;
import com.bloom.source.lib.utils.ByteUtil;
import com.bloom.common.constants.Constant.recordstatus;
import com.bloom.common.exc.AdapterException;

public abstract class BinaryResultSet
  extends ResultSet
{
  protected byte[] rowData;
  Logger logger = Logger.getLogger(BinaryResultSet.class);
  MetaData meta;
  Column[] columns;
  ByteBuffer internalBuffer;
  int recordBegin;
  int stringLength;
  long recordBeginOffset = 0L;
  long recordEndOffset = 0L;
  
  public BinaryResultSet(Reader reader, Property prop)
    throws IOException, InterruptedException
  {
    super(reader, prop);
  }
  
  public void metaData(MetaData meta)
  {
    this.meta = meta;
    this.columns = meta.colums();
  }
  
  protected void Init()
    throws IOException, InterruptedException
  {
    this.rowData = new byte[reader().blockSize()];
    this.internalBuffer = ByteBuffer.allocate(reader().blockSize() * 2);
    this.internalBuffer.flip();
    super.Init();
  }
  
  public recordstatus next()
    throws IOException, InterruptedException
  {
    this.recordBegin = this.internalBuffer.position();
    int colIdx;
    for (;;)
    {
      for (colIdx = 0; colIdx < this.columns.length; colIdx++)
      {
        if (this.internalBuffer.position() >= this.internalBuffer.limit()) {
          break;
        }
        this.columnOffset[colIdx] = this.internalBuffer.position();
        if (this.columns[colIdx].getType() != Constant.fieldType.STRING)
        {
          this.columnLength[colIdx] = this.columns[colIdx].getSize();
        }
        else
        {
          if (this.internalBuffer.limit() - this.internalBuffer.position() < 2) {
            break;
          }
          this.stringLength = ByteUtil.bytesToShort(this.internalBuffer.array(), this.columnOffset[colIdx]);
          this.stringLength -= this.columns[colIdx].lengthOffset();
          if (this.internalBuffer.position() + this.stringLength > this.internalBuffer.limit()) {
            break;
          }
          this.columnLength[colIdx] = this.stringLength;
        }
        if (this.columnOffset[colIdx] + this.columnLength[colIdx] > this.internalBuffer.limit()) {
          break;
        }
        this.internalBuffer.position(this.columnOffset[colIdx] + this.columnLength[colIdx]);
      }
      if (colIdx < this.columns.length)
      {
        if (this.recordBegin != 0)
        {
          this.internalBuffer.position(this.recordBegin);
          int sizeToCopy = this.internalBuffer.limit() - this.recordBegin;
          if (sizeToCopy > 0) {
            System.arraycopy(this.internalBuffer.array(), this.internalBuffer.position(), this.internalBuffer.array(), 0, sizeToCopy);
          }
          this.internalBuffer.limit(sizeToCopy);
          this.internalBuffer.position(0);
          this.recordBegin = 0;
        }
        else
        {
          this.internalBuffer.position(this.recordBegin);
        }
        try
        {
          ByteBuffer tmpBuffer = (ByteBuffer)this.reader.readBlock();
          if (tmpBuffer != null)
          {
            System.arraycopy(tmpBuffer.array(), 0, this.internalBuffer.array(), this.internalBuffer.limit(), tmpBuffer.limit());
            
            this.internalBuffer.limit(this.internalBuffer.limit() + tmpBuffer.limit());
            this.internalBuffer.position(0);
          }
          else
          {
            break label533;
          }
        }
        catch (AdapterException e)
        {
          e.printStackTrace();
        }
      }
    }
    setColumnCount(colIdx);
    colIdx--;
    this.recordCheckpoint.setRecordBeginOffset((this.recordCheckpoint.getRecordEndOffset() == null ? this.sourceCheckpoint.getRecordEndOffset() : this.recordCheckpoint.getRecordEndOffset()).longValue());
    this.recordEndOffset += this.columnOffset[colIdx] + this.columnLength[colIdx] - this.columnOffset[0];
    this.recordCheckpoint.setRecordEndOffset(this.sourceCheckpoint.getRecordEndOffset().longValue() + this.recordEndOffset);
    
    return recordstatus.VALID_RECORD;
    label533:
    return recordstatus.NO_RECORD;
  }
  
  public Object getColumnValue(int columnIndex)
  {
    if ((this.columnLength[columnIndex] != 0) && (this.columnOffset[columnIndex] != -1))
    {
      Object value = this.columns[columnIndex].getValue(this.internalBuffer.array(), this.columnOffset[columnIndex], this.columnLength[columnIndex]);
      Object retValue;
      switch (this.columns[columnIndex].getType())
      {
      case BYTE: 
        retValue = Byte.valueOf(((Byte)value).byteValue()); break;
      case DOUBLE: 
        retValue = Double.valueOf(((Double)value).doubleValue()); break;
      case FLOAT: 
        retValue = Float.valueOf(((Float)value).floatValue()); break;
      case LONG: 
        retValue = Double.valueOf(((Double)value).doubleValue()); break;
      case INTEGER: 
        retValue = Integer.valueOf(((Integer)value).intValue()); break;
      case SHORT: 
        retValue = Short.valueOf(((Short)value).shortValue()); break;
      case STRING: 
        retValue = (String)value; break;
      default: 
        System.out.println("Unhandled data type.");
        retValue = null;
      }
      return retValue;
    }
    return "";
  }
  
  public int copyColumnValue(byte[] buffer, int offset, int columnIndex)
  {
    if (buffer.length < this.columnLength[columnIndex]) {
      return -1;
    }
    System.arraycopy(this.internalBuffer.array(), this.columnOffset[columnIndex], buffer, offset, this.columnLength[columnIndex]);
    return this.columnLength[columnIndex];
  }
  
  public byte[] getRowData()
  {
    return this.rowData;
  }
  
  public void setRowData(byte[] rowData)
  {
    if (rowData != null) {
      this.rowData = Arrays.copyOf(rowData, rowData.length);
    }
  }
  
  public void update(Observable o, Object arg)
  {
    super.update(o, arg);
    switch ((com.bloom.source.lib.constant.Constant.eventType)arg)
    {
    case ON_OPEN: 
      setRecordCount(0);
      this.recordBeginOffset = 0L;
      this.recordEndOffset = 0L;
      if (this.logger.isInfoEnabled()) {
        this.logger.info(((ReaderBase)o).name() + " is opened ");
      }
      this.recordCheckpoint.seekPosition(0L);
      this.recordCheckpoint.setSourceName(reader().name());
      
      break;
    case ON_CLOSE: 
      if (this.logger.isInfoEnabled()) {
        this.logger.info("No of records in the file is " + getRecordCount());
      }
      if ((((ReaderBase)o).name() != null) && 
        (this.logger.isInfoEnabled())) {
        this.logger.info(((ReaderBase)o).name() + " is closed\n");
      }
      break;
    }
  }
}
