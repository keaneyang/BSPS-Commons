package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.log4j.Logger;

public class Enricher
  extends ReaderBase
{
  private boolean skipBom;
  private Logger logger = Logger.getLogger(Enricher.class);
  
  protected Enricher(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    this.skipBom = true;
    BOM.init();
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("Enricher layer is initialized");
    }
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
    ByteBuffer buffer = (ByteBuffer)this.linkedStrategy.readBlock();
    if (buffer != null) {
      try
      {
        if (this.skipBom)
        {
          BOM.skip(CharSet(), buffer);
          this.skipBom = false;
        }
      }
      catch (IOException e)
      {
        throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      }
    }
    return buffer;
  }
  
  protected void onClose(Object identifier)
  {
    super.onClose(identifier);
    this.skipBom = true;
  }
  
  protected void onOpen(Object identifier)
  {
    super.onOpen(identifier);
    this.skipBom = true;
  }
}
