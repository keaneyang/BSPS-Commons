package com.bloom.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.kafka.OLVPipedInputStream;
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;

public class KafkaPartitionReader
  extends StreamReader
{
  private OLVPipedInputStream olvStream;
  
  protected KafkaPartitionReader(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public KafkaPartitionReader(Property prop, InputStream input)
    throws AdapterException
  {
    super(prop, input);
    this.olvStream = ((OLVPipedInputStream)input);
    this.eventMetadataMap.put("TopicName", prop.getMap().get("TopicName"));
    this.eventMetadataMap.put("PartitionID", prop.getMap().get("PartitionID"));
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
    Object obj = super.readBlock();
    if ((this.olvStream.isNewMessage()) && (obj != null)) {
      onOpen(Constant.eventType.ON_OPEN);
    }
    return obj;
  }
  
  public void close()
    throws IOException
  {
    this.closeCalled = true;
    super.close();
  }
  
  public Map<String, Object> getEventMetadata()
  {
    this.eventMetadataMap.put("KafkaRecordOffset", Long.valueOf(this.olvStream.getKafkaMessageOffset()));
    return this.eventMetadataMap;
  }
  
  public String name()
  {
    String name = "Kafka Message offset-" + this.olvStream.getKafkaMessageOffset();
    return name;
  }
}
