package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;

public class GGTrailPositioner
  extends ReaderBase
{
  protected GGTrailPositioner(Property prop)
    throws AdapterException
  {
    super(prop);
  }
  
  public GGTrailPositioner(ReaderBase link)
    throws AdapterException
  {
    super(link);
    if (link != null) {
      this.linkedStrategy = link;
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return this.linkedStrategy.readBlock();
  }
  
  public void position(CheckpointDetail record, boolean position)
    throws AdapterException
  {
    super.position(record, position);
    if (record != null) {
      skipBytes(record.getRecordEndOffset().longValue());
    }
    this.recoveryCheckpoint = new CheckpointDetail(record);
  }
}
