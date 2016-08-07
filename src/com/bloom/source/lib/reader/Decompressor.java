package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.log4j.Logger;

public class Decompressor
  extends StreamReader
{
  private Logger logger = Logger.getLogger(Decompressor.class);
  boolean failedDuringInit;
  int retryCnt;
  InputStream failedStream;
  Map<String, String> typeMap = new HashMap()
  {
    private static final long serialVersionUID = -5991261969577676448L;
  };
  String compressionType;
  
  protected Decompressor(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    this.compressionType = this.linkedStrategy.property().getString(Property.COMPRESSION_TYPE, "").toLowerCase();
    if (this.typeMap.get(this.compressionType) != null) {
      this.compressionType = ((String)this.typeMap.get(this.compressionType));
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    try
    {
      if ((this.dataSource == null) || (this.failedDuringInit))
      {
        InputStream ip = null;
        if (this.failedDuringInit) {
          ip = this.failedStream;
        } else {
          ip = this.linkedStrategy.getInputStream();
        }
        if (ip != null)
        {
          if (this.logger.isTraceEnabled()) {
            this.logger.trace("Decompression layer is initialized with compression type {" + this.compressionType + "}");
          }
          this.failedStream = ip;
          this.dataSource = new CompressorStreamFactory().createCompressorInputStream(this.compressionType, ip);
          this.failedStream = null;
          this.failedDuringInit = false;
          this.retryCnt = 0;
        }
        else
        {
          this.dataSource = null;
        }
      }
    }
    catch (CompressorException e)
    {
      if (this.closeCalled)
      {
        if (this.logger.isDebugEnabled()) {
          this.logger.debug("Closed is called, returning null");
        }
        return null;
      }
      if (this.retryCnt < 5)
      {
        try
        {
          if (!this.failedStream.markSupported())
          {
            if ((this.failedStream instanceof FileInputStream))
            {
              FileChannel fc = ((FileInputStream)this.failedStream).getChannel();
              fc.position(0L);
            }
          }
          else {
            this.failedStream.reset();
          }
        }
        catch (IOException ioExp)
        {
          this.logger.warn("Got exception while positioning the stream to begining {" + ioExp.getMessage() + "}");
        }
        this.failedDuringInit = true;
        this.retryCnt += 1;
        if (this.logger.isTraceEnabled()) {
          this.logger.trace("Got excepption in Decompressor: {" + e.getCause().getMessage() + "} this could be due to partial write. Retry attempt will be made at reading from the begining {" + name() + "} again");
        }
        return null;
      }
      InputStream nextStream = this.linkedStrategy.getInputStream();
      if (nextStream != null)
      {
        this.logger.warn("Retried reinitializing Decompressor {" + this.retryCnt + "} times, moving to {" + name() + "}");
        this.failedStream = nextStream;
        this.failedDuringInit = true;
      }
      else
      {
        this.retryCnt = 0;
        this.failedDuringInit = true;
      }
      try
      {
        Thread.sleep(100L);
      }
      catch (InterruptedException e1) {}
      return null;
    }
    if (this.dataSource != null)
    {
      try
      {
        return super.readBlock();
      }
      catch (AdapterException exp)
      {
        if (exp.getType() == Error.END_OF_DATASOURCE)
        {
          if (this.dataSource != null)
          {
            try
            {
              this.dataSource.close();
            }
            catch (IOException e)
            {
              e.printStackTrace();
            }
            this.dataSource = null;
          }
        }
        else {
          throw exp;
        }
      }
      return null;
    }
    return null;
  }
  
  public void close()
  {
    try
    {
      super.close();
    }
    catch (IOException e)
    {
      e.printStackTrace();
    }
  }
  
  public void position(CheckpointDetail recordAttribute, boolean position)
    throws AdapterException
  {
    if (recordAttribute != null)
    {
      recordAttribute.setRecordBeginOffset(0L);
      recordAttribute.setRecordLength(0L);
      recordAttribute.setRecordEndOffset(0L);
    }
    if (this.linkedStrategy != null) {
      this.linkedStrategy.position(recordAttribute, position);
    }
  }
  
  public Map<String, Object> getEventMetadata()
  {
    if (this.linkedStrategy != null) {
      return this.linkedStrategy.getEventMetadata();
    }
    return this.eventMetadataMap;
  }
}
