package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import org.apache.log4j.Logger;

public class StreamReader
  extends ReaderBase
{
  protected InputStream dataSource;
  boolean closeCalled;
  boolean isPipedInputStream;
  private Logger logger = Logger.getLogger(StreamReader.class);
  
  protected StreamReader(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public StreamReader(InputStream input)
    throws AdapterException
  {
    super((ReaderBase)null);
    this.dataSource = input;
    this.isPipedInputStream = isPipedInputStream(input);
  }
  
  public StreamReader(Property prop, InputStream input)
    throws AdapterException
  {
    super(prop);
    this.property = prop;
    this.dataSource = input;
    this.isPipedInputStream = isPipedInputStream(input);
    if (prop.getMap().get("EventMetadata") != null) {
      this.eventMetadataMap = ((Map)prop.getMap().get("EventMetadata"));
    }
  }
  
  private boolean isPipedInputStream(InputStream in)
  {
    if ((in instanceof PipedInputStream)) {
      return true;
    }
    return false;
  }
  
  protected void init()
    throws AdapterException
  {
    super.init();
  }
  
  public void setCheckPointDetails(CheckpointDetail cp)
  {
    if (this.linkedStrategy != null) {
      this.linkedStrategy.setCheckPointDetails(cp);
    } else {
      this.recoveryCheckpoint = cp;
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    try
    {
      if ((!this.closeCalled) && ((!this.isPipedInputStream) || (this.dataSource.available() > 0)))
      {
        int bytes = this.dataSource.read(this.internalBuffer.array(), 0, this.internalBuffer.capacity());
        if (bytes >= 0)
        {
          if (this.logger.isTraceEnabled()) {
            this.logger.trace("{" + bytes + "} bytes have been read from {" + name() + "}");
          }
          this.internalBuffer.position(0);
          this.internalBuffer.limit(bytes);
          return this.internalBuffer;
        }
        throw new AdapterException(Error.END_OF_DATASOURCE);
      }
      return null;
    }
    catch (IOException e)
    {
      if (!this.closeCalled) {
        throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      }
    }
    return null;
  }
  
  public void setInputStream(InputStream dataSource)
    throws AdapterException
  {
    if (this.dataSource != null)
    {
      try
      {
        close();
      }
      catch (IOException e)
      {
        throw new AdapterException(Error.GENERIC_EXCEPTION, e);
      }
      this.dataSource = null;
    }
    this.dataSource = dataSource;
  }
  
  public InputStream getInputStream()
  {
    InputStream tmpStream = this.dataSource;
    this.dataSource = null;
    return tmpStream;
  }
  
  public void close()
    throws IOException
  {
    this.closeCalled = true;
    if (this.dataSource != null) {
      this.dataSource.close();
    }
    super.close();
  }
  
  public int available()
    throws IOException
  {
    return this.dataSource.available();
  }
  
  public Map<String, Object> getEventMetadata()
  {
    return this.eventMetadataMap;
  }
}
