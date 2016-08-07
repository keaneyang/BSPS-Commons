package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.intf.EventMetadataProvider;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.errors.Error;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.CharBuffer;
import java.util.Map;
import java.util.Observer;
import org.apache.log4j.Logger;

public class Reader
  extends InputStream
  implements EventMetadataProvider
{
  public static String READER_TYPE = Property.READER_TYPE;
  public static String FILE_READER = "FileReader";
  public static String MULTI_FILE_READER = "MultiFileReader";
  public static String UDP_READER = "UDPReader";
  public static String TCP_READER = "TCPReader";
  public static String JMS_READER = "JMSReader";
  public static String HDFS_READER = "HDFSReader";
  public static String STREAM_READER = "StreamReader";
  public static String STREAM = "Stream";
  public static String STRING_READER = "StringReader";
  public static String ENCODER = "Encoder";
  public static String DECOMPRESSOR = "Decompressor";
  public static String ENRICHER = "Enricher";
  public static String ARCHIVE_EXTRACTOR = "ArchiveExtractor";
  public static String THREADED_QUEUE = "ThreadedQueueReader";
  public static String KAFKA_READER = "KafkaReader";
  public static String RECOVERY_MODE = "RecoveryMode";
  private Logger logger = Logger.getLogger(Reader.class);
  private ReaderBase strategy;
  private long bytesRead = 0L;
  private boolean readerClosed = false;
  public final int DEFAULT_BLOCK_SIZE = 4096;
  
  public Reader(ReaderBase strategy)
  {
    this.strategy = strategy;
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("Reader is initialized with the following strategy " + strategy.name);
    }
  }
  
  public String name()
  {
    return this.strategy.name();
  }
  
  public Object readBlock()
    throws AdapterException
  {
    return this.strategy.readBlock();
  }
  
  public Object readBock(boolean multiEndpointSupport)
    throws AdapterException
  {
    return this.strategy.readBlock(multiEndpointSupport);
  }
  
  public int blockSize()
  {
    return this.strategy.blockSize();
  }
  
  public void blockSize(int block_size)
  {
    this.strategy.blockSize(block_size);
  }
  
  public void close()
    throws IOException
  {
    if (!this.readerClosed)
    {
      this.strategy.close();
      this.readerClosed = true;
    }
  }
  
  public boolean isDone()
  {
    return false;
  }
  
  public int read()
    throws IOException
  {
    return this.strategy.read();
  }
  
  public int available()
    throws IOException
  {
    return this.strategy.available();
  }
  
  public CheckpointDetail getCheckpointDetail()
  {
    return this.strategy.getCheckpointDetail();
  }
  
  public long charactersToSkip()
  {
    return this.strategy.bytesToSkip();
  }
  
  public long skip(long characterOffset)
    throws IOException
  {
    return this.strategy.skip(characterOffset);
  }
  
  public int read(byte[] buffer, int off, int len)
  {
    int bytesRead = this.strategy.read(buffer, off, len);
    if (bytesRead > 0)
    {
      this.bytesRead += bytesRead;
      this.strategy.recoveryCheckpoint.setBytesRead(this.bytesRead);
    }
    return bytesRead;
  }
  
  public long skipBytes(long offset)
    throws AdapterException
  {
    return this.strategy.skipBytes(offset);
  }
  
  public void position(CheckpointDetail record, boolean position)
    throws AdapterException
  {
    this.strategy.position(record, position);
  }
  
  public int eofdelay()
  {
    return this.strategy.eofdelay();
  }
  
  public ReaderBase strategy()
  {
    return this.strategy;
  }
  
  public boolean supportsMutipleEndpoint()
  {
    return this.strategy.supportsMutipleEndpoint();
  }
  
  public void registerObserver(Object observer)
  {
    this.strategy.registerObserver((Observer)observer);
  }
  
  public static Reader TCPReader(Property prop)
    throws AdapterException
  {
    ReaderBase tcpReader = new TCPReader(prop);
    tcpReader.init();
    return new Reader(tcpReader);
  }
  
  public static Reader TCPBufferedReader(Property prop)
    throws AdapterException
  {
    ReaderBase tcpBufferedReader = new QueueReader(new TCPReader(prop));
    tcpBufferedReader.init();
    return new Reader(tcpBufferedReader);
  }
  
  public static Reader UDPReader(Property prop)
    throws AdapterException
  {
    ReaderBase udpReader = new UDPReader(prop);
    udpReader.init();
    return new Reader(udpReader);
  }
  
  public static Reader UDPBufferedReader(Property prop)
    throws AdapterException
  {
    ReaderBase udpBufferedReader = new QueueReader(new UDPReader(prop));
    udpBufferedReader.init();
    return new Reader(udpBufferedReader);
  }
  
  public static Reader CharUDPReader(Property prop)
    throws AdapterException
  {
    ReaderBase charUDPReader = new Encoding(new UDPReader(prop));
    charUDPReader.init();
    return new Reader(charUDPReader);
  }
  
  public static Reader JMSReader(Property prop)
    throws AdapterException
  {
    ReaderBase jmsReader = new JMSReader(prop);
    jmsReader.init();
    return new Reader(jmsReader);
  }
  
  public static Reader CharJMSReader(Property prop)
    throws AdapterException
  {
    ReaderBase charJMSReader = new Encoding(new JMSReader(prop));
    charJMSReader.init();
    return new Reader(charJMSReader);
  }
  
  public static Reader UDPThreadedReader(Property prop)
    throws AdapterException
  {
    ReaderBase udpThreadedReader = new ThreadedQueueReader(new UDPReader(prop));
    udpThreadedReader.init();
    return new Reader(udpThreadedReader);
  }
  
  public static Reader CharUDPThreadedReader(Property prop)
    throws AdapterException
  {
    ReaderBase charUDPThreadedReader = new Encoding(new ThreadedQueueReader(new UDPReader(prop)));
    charUDPThreadedReader.init();
    return new Reader(charUDPThreadedReader);
  }
  
  public static Reader CharTCPBufferedReader(Property prop)
    throws AdapterException
  {
    return new Reader(new CharBufferManager(new Encoding(new ThreadedQueueReader(new TCPReader(prop)))));
  }
  
  public static Reader TCPThreadedReader(Property prop)
    throws AdapterException
  {
    ReaderBase tcpThreadedReader = new ThreadedQueueReader(new TCPReader(prop));
    tcpThreadedReader.init();
    return new Reader(tcpThreadedReader);
  }
  
  public static Reader CharTCPThreadedReader(Property prop)
    throws AdapterException
  {
    ReaderBase charTCPThreadedReader = new Encoding(new ThreadedQueueReader(new TCPReader(prop)));
    charTCPThreadedReader.init();
    return new Reader(charTCPThreadedReader);
  }
  
  public static Reader FileReader(Property prop)
    throws AdapterException
  {
    ReaderBase fileReader = new FileReader(prop);
    fileReader.init();
    return new Reader(fileReader);
  }
  
  public static Reader CharFileReader(Property prop)
    throws AdapterException
  {
    ReaderBase charFileReader = new Encoding(new FileReader(prop));
    charFileReader.init();
    return new Reader(charFileReader);
  }
  
  public static Reader FileBufferedReader(Property prop)
    throws AdapterException
  {
    ReaderBase fileBufferedReader = new QueueReader(new FileReader(prop));
    fileBufferedReader.init();
    return new Reader(fileBufferedReader);
  }
  
  public static Reader XMLPositioner(Reader reader)
    throws AdapterException
  {
    return new Reader(new XMLPositioner(reader.strategy()));
  }
  
  public static Reader GGTrailPositioner(Reader reader)
    throws AdapterException
  {
    return new Reader(new GGTrailPositioner(reader.strategy()));
  }
  
  public static Reader addStrategy(Reader reader, String strategyType)
    throws AdapterException
  {
    ReaderBase tmp = createInstance(strategyType, null, null);
    tmp.init(reader.strategy().property());
    return addStrategy(reader, tmp);
  }
  
  public static Reader addStrategy(Reader reader, ReaderBase anexture)
    throws AdapterException
  {
    anexture.downstream(reader.strategy());
    reader.strategy().upstream(anexture);
    return new Reader(anexture);
  }
  
  public static Reader createInstance(Property prop)
    throws AdapterException
  {
    return createInstance(prop, false);
  }
  
  public static Reader createInstance(Property prop, boolean recoveryMode)
    throws AdapterException
  {
    if (recoveryMode) {
      prop.getMap().put(RECOVERY_MODE, Boolean.valueOf(true));
    } else {
      prop.getMap().put(RECOVERY_MODE, Boolean.valueOf(false));
    }
    ReaderBase reader = createInstance(prop.getString(Property.READER_TYPE, null), prop, null);
    String compressionType = prop.getString(Property.COMPRESSION_TYPE, null);
    reader = createInstance((compressionType != null) && (!compressionType.isEmpty()) ? DECOMPRESSOR : null, prop, reader);
    reader = createInstance(prop.getString(Property.ARCHIVE_TYPE, null) != null ? ARCHIVE_EXTRACTOR : null, prop, reader);
    reader = createInstance(prop.getBoolean("skipbom", true) ? ENRICHER : null, null, reader);
    reader = createInstance(prop.getString(Property.CHARSET, null) != null ? ENCODER : null, null, reader);
    reader.init();
    
    return new Reader(reader);
  }
  
  public static ReaderBase createInstance(String type, Property prop, ReaderBase link)
    throws AdapterException
  {
    if (type == null) {
      return link;
    }
    if (type.equals(FILE_READER))
    {
      if (prop.getBoolean(Property.NETWORK_FILE_SYSTEM, false)) {
        return new NFSReader(prop);
      }
      return new FileReader(prop);
    }
    if (type.equals(JMS_READER)) {
      return createInstance(THREADED_QUEUE, prop, new JMSReader(prop));
    }
    if (type.equals(TCP_READER)) {
      return createInstance(THREADED_QUEUE, prop, new TCPReader(prop));
    }
    if (type.equals(UDP_READER)) {
      return createInstance(THREADED_QUEUE, prop, new UDPReader(prop));
    }
    if (type.equals(HDFS_READER)) {
      return new HDFSReader(prop);
    }
    if (type.equals(KAFKA_READER)) {
      return new KafkaPartitionReader(prop, (InputStream)prop.getMap().get(STREAM));
    }
    if (type.equals(STREAM_READER)) {
      return new StreamReader(prop, (InputStream)prop.getMap().get(STREAM));
    }
    if (type.equals(ENCODER)) {
      return new Encoding(link);
    }
    if (type.equals(ENRICHER)) {
      return new Enricher(link);
    }
    if (type.equals(DECOMPRESSOR)) {
      return new Decompressor(link);
    }
    if (type.equals(ARCHIVE_EXTRACTOR)) {
      return new ArchiveExtractor(link);
    }
    if (type.equals(THREADED_QUEUE)) {
      return new ThreadedQueueReader(link);
    }
    if (type.equals(STRING_READER)) {
      return new StringReader(prop);
    }
    throw new AdapterException("No object is mapped for [" + type + "]");
  }
  
  public static InputStream convertStringToInputStream(String data, String charset)
    throws AdapterException
  {
    if ((data == null) || (data.isEmpty())) {
      throw new AdapterException("String to be converted cannot be null or empty");
    }
    charset = charset != null ? charset : "UTF-8";
    try
    {
      return new ByteArrayInputStream(data.getBytes(charset));
    }
    catch (UnsupportedEncodingException e)
    {
      throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME, e);
    }
  }
  
  public void setInputStream(InputStream dataSource)
    throws AdapterException
  {
    this.strategy.setInputStream(dataSource);
  }
  
  public void setCharacterBuffer(CharBuffer buffer)
  {
    this.strategy.setCharacterBuffer(buffer);
  }
  
  public Map<String, Object> getEventMetadata()
  {
    return this.strategy.getEventMetadata();
  }
  
  public String toString()
  {
    return name();
  }
}
