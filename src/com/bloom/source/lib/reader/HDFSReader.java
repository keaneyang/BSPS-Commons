package com.bloom.source.lib.reader;

import com.bloom.source.lib.directory.Directory;
import com.bloom.source.lib.directory.HDFSDirectory;
import com.bloom.source.lib.io.common.HDFSCommon;
import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.utils.DefaultFileComparator;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;
import com.bloom.common.exc.RecordException.Type;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Map;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class HDFSReader
  extends FileReader
{
  String hadoopUri;
  FileSystem hadoopFileSystem;
  Logger logger = Logger.getLogger(HDFSReader.class);
  private HDFSCommon hdfsCommon;
  Directory dir;
  public static final int MAX_RETRY_CNT = 5;
  
  public HDFSReader(Property prop)
    throws AdapterException
  {
    super(prop);
    this.prop = prop;
    this.hdfsCommon = new HDFSCommon(prop);
  }
  
  public void init()
    throws AdapterException
  {
    this.hadoopFileSystem = this.hdfsCommon.getHDFSInstance();
    super.init();
    hdfsinit();
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("HDFSReader is initialized with following properties\nHadoopURL - [" + this.prop.hadoopUrl + "]\n" + "Wildcard - [" + this.prop.wildcard + "]\n" + "EOFDelay - [" + this.prop.eofdelay + "]\n" + "PositionByEOF - [" + this.prop.positionByEOF + "]\n" + "RollOverPolicy - [" + this.prop.rollOverPolicy + "]");
    }
  }
  
  private void hdfsinit()
    throws AdapterException
  {
    this.buffer = ByteBuffer.allocate(blockSize());
    this.buffer.clear();
    this.positionByEOF = this.prop.positionByEOF;
    this.skipBOM = this.prop.skipBOM;
    this.breakOnNoRecord = this.prop.getBoolean("breakonnorecord", false);
    this.dir = createDirectoryInstance();
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("FileReader is initialized with following properties\nDirectory - [" + this.prop.directory + "]\n" + "Wildcard - [" + this.prop.wildcard + "]\n" + "Blocksize - [" + this.prop.blocksize + "]\n" + "PositionByEOF - [" + this.prop.positionByEOF + "]\n" + "RollOverPolicy - [" + this.prop.rollOverPolicy + "]");
    }
  }
  
  protected Directory createDirectoryInstance()
    throws AdapterException
  {
    String rollOverPolicy = this.prop.rollOverPolicy;
    Comparator<Object> policy = null;
    try
    {
      if (rollOverPolicy != null)
      {
        Class<?> obj = Class.forName("com.bloom.source.lib.utils." + rollOverPolicy);
        policy = (DefaultFileComparator)obj.getConstructor(new Class[] { FileSystem.class }).newInstance(new Object[] { this.hadoopFileSystem });
      }
      return new HDFSDirectory(this.prop, this.hadoopFileSystem, policy);
    }
    catch (InstantiationException|IllegalAccessException|ClassNotFoundException|IllegalArgumentException|InvocationTargetException|NoSuchMethodException|SecurityException|IOException|InterruptedException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      throw se;
    }
  }
  
  protected boolean openFile()
    throws AdapterException
  {
    if (this.dir.getFileListSize() > 0)
    {
      if (this.recovery)
      {
        this.currentFile = this.dir.getFile(this.fileToBeRecovered);
        if (this.currentFile == null) {
          return false;
        }
      }
      else
      {
        if (this.positionByEOF) {
          while (!this.dir.isLastFile()) {
            this.currentFile = this.dir.getNextFile();
          }
        }
        this.currentFile = this.dir.getNextFile();
      }
      return open(this.currentFile);
    }
    return false;
  }
  
  protected boolean open(Object file)
    throws AdapterException
  {
    Path fileToBeOpened = (Path)file;
    String hadoopUrl = this.hdfsCommon.getHadoopUrl();
    this.hadoopUri = (hadoopUrl + fileToBeOpened.getName());
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("File [" + fileToBeOpened.getName() + "] opened from HDFS directory [" + hadoopUrl + "]");
    }
    try
    {
      this.eventMetadataMap.put("FileName", this.hadoopUri);
      if (this.positionByEOF)
      {
        this.eofPosition = this.hadoopFileSystem.getFileStatus(fileToBeOpened).getLen();
        this.eventMetadataMap.put("FileOffset", Long.valueOf(this.eofPosition));
      }
      else
      {
        this.eventMetadataMap.put("FileOffset", Integer.valueOf(0));
      }
      this.inputStream = this.hadoopFileSystem.open(new Path(this.hadoopUri));
    }
    catch (IllegalArgumentException|IOException e)
    {
      AdapterException se = new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
      throw se;
    }
    return true;
  }
  
  protected boolean isEOFReached()
    throws AdapterException
  {
    try
    {
      if (isEOFReached(this.inputStream, this.fileChanged))
      {
        if (openFile()) {
          return true;
        }
      }
      else if (this.fileChanged)
      {
        this.inputStream.close();
        this.inputStream = null;
        printCloseTrace();
      }
    }
    catch (IOException e)
    {
      String eMsg = "Got exception while checking for EOF {" + ((File)this.currentFile).getName() + "}";
      if (this.breakOnNoRecord) {
        throw new RuntimeException(eMsg, new RecordException(eMsg, RecordException.Type.END_OF_DATASOURCE));
      }
      throw new AdapterException(eMsg, e);
    }
    return false;
  }
  
  public boolean isEOFReached(InputStream inputStream, boolean fileChanged)
    throws IOException
  {
    if (((fileChanged) || (inputStream.available() <= 0)) && 
      (this.dir.getFileListSize() > 0))
    {
      if (((fileChanged) || (inputStream.available() <= 0)) && (this.retryCnt < 5))
      {
        if ((fileChanged) || (inputStream.available() <= 0)) {
          this.retryCnt += 1;
        } else {
          this.retryCnt = 0;
        }
        return false;
      }
      this.retryCnt = 0;
      return true;
    }
    return false;
  }
  
  public void close()
    throws IOException
  {
    super.close();
    if (this.hadoopFileSystem != null) {
      this.hadoopFileSystem.close();
    }
  }
  
  public Map<String, Object> getEventMetadata()
  {
    return this.eventMetadataMap;
  }
}
