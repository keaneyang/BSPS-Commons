package com.bloom.source.lib.reader;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.log4j.Logger;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.constant.Constant;
import com.bloom.source.lib.directory.FileBank;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.exc.RecordException;

public class FileReader
  extends ReaderBase
{
  protected FileBank fb;
  protected Object currentFile;
  protected ByteBuffer buffer;
  protected int retryCnt = 0;
  protected int MAX_RETRY = 5;
  private Logger logger = Logger.getLogger(FileReader.class);
  protected long eofPosition = 0L;
  protected long creationTime;
  protected boolean positionByEOF = true;
  protected boolean recovery = false;
  protected boolean skipBOM;
  protected String fileToBeRecovered;
  protected boolean startFromBegining;
  protected long bytesRead = 0L;
  protected long readByteAddress = 0L;
  protected long previousBytesRead = 0L;
  protected Property prop;
  protected boolean isFirstTime = true;
  protected final String ROLL_OVER_PACAKGE = "com.bloom.source.lib.utils.";
  boolean closeCalled;
  boolean breakOnNoRecord;
  protected long bytesConsumed;
  protected String fileKey = "";
  protected boolean fileChanged = false;
  
  public FileReader(Property prop)
    throws AdapterException
  {
    super(prop);
    this.prop = prop;
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    this.buffer = ByteBuffer.allocate(blockSize());
    this.buffer.clear();
    this.positionByEOF = this.prop.positionByEOF;
    this.skipBOM = this.prop.skipBOM;
    this.breakOnNoRecord = this.prop.getBoolean("breakonnorecord", false);
    if (!(this instanceof HDFSReader))
    {
      this.fb = ((FileBank)this.prop.getObject(FileBank.INSTANCE, null));
      if (this.fb == null)
      {
        this.fb = new FileBank(this.prop);
        this.fb.start();
      }
      else
      {
        this.fb.updateInstance(this.prop);
      }
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("FileReader is initialized with following properties\nDirectory - [" + this.prop.directory + "]\n" + "Wildcard - [" + this.prop.wildcard + "]\n" + "Blocksize - [" + this.prop.blocksize + "]\n" + "PositionByEOF - [" + this.prop.positionByEOF + "]\n" + "RollOverPolicy - [" + this.prop.rollOverPolicy + "]");
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    int bytesRead = 0;
    if (this.inputStream != null)
    {
      do
      {
        try
        {
          this.buffer.clear();
          bytesRead = this.inputStream.read(this.buffer.array());
        }
        catch (IOException exp)
        {
          if (this.closeCalled)
          {
            if (this.logger.isDebugEnabled()) {
              this.logger.debug("Got exception because of close() is called in different thread");
            }
            return null;
          }
          this.logger.error("File : {" + this.name + "}");
          this.logger.error("FileReader IOException : {" + exp.getMessage() + "}");
          throw new AdapterException(Error.GENERIC_IO_EXCEPTION, exp);
        }
        if (bytesRead > 0) {
          break;
        }
      } while (isEOFReached());
      if (this.logger.isTraceEnabled()) {
        this.logger.trace(bytesRead + " bytes have been read from file " + name() + " in the directory " + this.prop.directory);
      }
      this.buffer.limit(bytesRead);
      this.bytesConsumed += bytesRead;
      this.bytesRead += bytesRead;
      this.recoveryCheckpoint.setBytesRead(this.bytesRead);
      return this.buffer;
    }
    position(null, false);
    return null;
  }
  
  public void close()
    throws IOException
  {
    super.close();
    this.closeCalled = true;
    if (this.inputStream != null)
    {
      this.inputStream.close();
      printCloseTrace();
    }
    if (this.fb != null) {
      this.fb.stop(this.prop.wildcard);
    }
    notifyObservers(Constant.eventType.ON_CLOSE);
  }
  
  public long skip(long bytesToSkip)
    throws IOException
  {
    if (this.inputStream != null) {
      return this.inputStream.skip(bytesToSkip);
    }
    return -1L;
  }
  
  protected boolean openFile()
    throws AdapterException
  {
    FileBank.FileDetails fd = this.fb.getNextFile(this.prop.wildcard, (File)this.currentFile, this.bytesConsumed);
    if (fd != null)
    {
      if (this.inputStream != null)
      {
        printCloseTrace();
        try
        {
          this.inputStream.close();
        }
        catch (IOException e)
        {
          throw new AdapterException("Got exception while closing inputstream {" + ((File)this.currentFile).getName() + "}", e);
        }
      }
      Object prevFile = this.currentFile;
      this.currentFile = fd.file;
      this.logger.debug("Got file {" + fd.file.getName() + "} position {" + fd.startPosition + "} from Sequencer");
      if (open(this.currentFile))
      {
        this.fileChanged = false;
        long startPosition = fd.startPosition;
        if (startPosition != 0L) {
          try
          {
            this.inputStream.skip(startPosition);
          }
          catch (IOException e)
          {
            throw new AdapterException("Got exception while positioning inputstream for {" + startPosition + "}", e);
          }
        }
        this.bytesConsumed = startPosition;
        this.recoveryCheckpoint.seekPosition(startPosition);
        this.eventMetadataMap.put("FileOffset", Long.valueOf(startPosition));
        this.logger.debug("Setting BytesConsumed as {" + this.bytesConsumed + "}");
        return true;
      }
      this.logger.debug("File open failed for {" + ((File)this.currentFile).getName() + "} put back the old file");
      this.currentFile = prevFile;
    }
    return false;
  }
  
  protected boolean open(Object file)
    throws AdapterException
  {
    if (file == null) {
      return false;
    }
    boolean fileOpen = true;
    File fileToBeOpened = (File)file;
    name(fileToBeOpened.getName());
    try
    {
      this.inputStream = new FileInputStream(fileToBeOpened);
      if (this.logger.isTraceEnabled()) {
        this.logger.trace("File [" + fileToBeOpened.getName() + "] opened from the directory [" + this.prop.directory + "]");
      }
      onOpen(fileToBeOpened.getName());
      setChanged();
      notifyObservers(Constant.eventType.ON_OPEN);
      setFileDetails(fileToBeOpened);
      return true;
    }
    catch (FileNotFoundException e)
    {
      String errMsg = "File :{" + fileToBeOpened.getName() + "} is not found";
      this.logger.warn(errMsg);
      AdapterException se = new AdapterException(errMsg, e);
      throw se;
    }
    catch (AdapterException e)
    {
      if (e.getType() != Error.BROKEN_LINK) {
        throw e;
      }
      this.logger.warn("Got SourceException {" + e.getMessage() + "} while opening {" + fileToBeOpened.getName() + "}");
      fileOpen = false;
      this.inputStream = null;
    }
    return fileOpen;
  }
  
  protected boolean isEOFReached()
    throws AdapterException
  {
    try
    {
      if ((this.fb.isEOFReached(this.prop.wildcard, this.inputStream, this.fileChanged)) && 
        (openFile())) {
        return true;
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
  
  public long getEOFPosition()
  {
    return this.eofPosition;
  }
  
  public long getCreationTime()
  {
    return this.creationTime;
  }
  
  public InputStream getInputStream()
    throws AdapterException
  {
    if (this.isFirstTime)
    {
      this.isFirstTime = false;
      if (this.inputStream != null) {
        return this.inputStream;
      }
    }
    if (openFile()) {
      return this.inputStream;
    }
    return null;
  }
  
  private void setFileDetails(File file)
    throws AdapterException
  {
    Path filePath = file.toPath();
    try
    {
      BasicFileAttributes attributes = Files.readAttributes(filePath, BasicFileAttributes.class, new LinkOption[0]);
      this.eofPosition = 0L;
      this.eventMetadataMap.put("FileName", file.getName());
      if (this.positionByEOF)
      {
        this.eofPosition = attributes.size();
        this.eventMetadataMap.put("FileOffset", Long.valueOf(this.eofPosition));
      }
      else
      {
        this.eventMetadataMap.put("FileOffset", Integer.valueOf(0));
      }
      this.creationTime = attributes.creationTime().toMillis();
      this.recoveryCheckpoint.setSourceName(file.getName());
      this.recoveryCheckpoint.setSourceCreationTime(attributes.creationTime().toMillis());
      if (attributes.fileKey() != null) {
        this.fileKey = attributes.fileKey().toString();
      } else {
        this.fileKey = "";
      }
    }
    catch (IOException e)
    {
      if (((e instanceof NoSuchFileException)) && 
        (Files.isSymbolicLink(filePath)))
      {
        this.logger.warn("Broken link found " + file.getAbsolutePath());
        throw new AdapterException("Symbolic link {" + file.getName() + "} is broken", e);
      }
      String errMsg = "Got IOException while retriving file attribute of {" + file.getName() + "}";
      this.logger.warn(errMsg);
      throw new AdapterException(errMsg, e);
    }
  }
  
  public long skipBytes(long offset)
    throws AdapterException
  {
    try
    {
      if (this.inputStream != null) {
        if ((this.inputStream instanceof FSDataInputStream)) {
          ((FSDataInputStream)this.inputStream).seek(offset);
        } else {
          ((FileInputStream)this.inputStream).getChannel().position(offset);
        }
      }
    }
    catch (IOException e)
    {
      String errMsg = "Got IOException while positioning file stream {" + name() + "} offset {" + offset + "}";
      this.logger.warn(errMsg);
      throw new AdapterException(errMsg, e);
    }
    return offset;
  }
  
  public void position(CheckpointDetail record, boolean position)
    throws AdapterException
  {
    if (record != null)
    {
      record.dump();
      this.recovery = true;
      this.recoveryCheckpoint.setRecovery(this.recovery);
      if (record.getRecordEndOffset() != null) {
        this.recoveryCheckpoint.setRecordEndOffset(record.getRecordBeginOffset().longValue());
      }
      this.fileToBeRecovered = record.getSourceName();
      if ((!this.fb.position(this.prop.wildcard, this.fileToBeRecovered)) || (!openFile()))
      {
        String errMsg = "Trying to recovery from file {" + name() + "} which is not found";
        this.logger.warn(errMsg);
        AdapterException se = new AdapterException(errMsg);
        throw se;
      }
      if (position)
      {
        validateSource(record);
        skipBytes(record.seekPosition().longValue());
        this.recoveryCheckpoint.seekPosition(record.seekPosition().longValue());
      }
    }
    else
    {
      this.recoveryCheckpoint.setRecovery(this.recovery);
      if (this.positionByEOF)
      {
        FileBank.FileDetails fd;
        do
        {
          fd = this.fb.getNextFile(this.prop.wildcard, null, 0L);
          if (fd != null) {
            this.currentFile = fd.file;
          }
        } while (fd != null);
        if (this.currentFile == null)
        {
          this.positionByEOF = false;
          return;
        }
      }
      try
      {
        if (((this.positionByEOF) && (this.currentFile != null) && (!open(this.currentFile))) || ((!this.positionByEOF) && (!openFile())))
        {
          if (this.logger.isDebugEnabled()) {
            this.logger.debug("Input directory is empty");
          }
          this.startFromBegining = true;
        }
        else
        {
          if (((this.positionByEOF) || (position)) && (this.startFromBegining != true))
          {
            if ((this.currentFile instanceof File)) {
              setFileDetails((File)this.currentFile);
            }
            skipBytes(this.eofPosition);
            if ((this.currentFile instanceof File)) {
              this.logger.debug("Positioned {" + ((File)this.currentFile).getName() + "} at {" + this.eofPosition + "}");
            }
            this.bytesConsumed = this.eofPosition;
            this.recoveryCheckpoint.seekPosition(this.eofPosition);
          }
          else
          {
            this.recoveryCheckpoint.seekPosition(0L);
          }
          this.startFromBegining = false;
        }
      }
      catch (AdapterException e)
      {
        throw e;
      }
    }
  }
  
  private void validateSource(CheckpointDetail record)
    throws AdapterException
  {
    if (this.eofPosition < record.seekPosition().longValue())
    {
      String errMsg = "File {" + name() + "} seems to be truncated. Trying to position at {" + record.seekPosition() + "} but it has got only  {" + this.eofPosition + "} bytes";
      this.logger.warn(errMsg);
      throw new AdapterException(errMsg);
    }
  }
  
  protected void printCloseTrace()
  {
    if (this.logger.isTraceEnabled())
    {
      if (this.readByteAddress != 0L)
      {
        this.readByteAddress = (this.bytesRead - this.previousBytesRead);
        this.previousBytesRead = this.bytesRead;
      }
      else
      {
        this.readByteAddress = (this.previousBytesRead = this.bytesRead);
      }
      this.logger.trace("File [" + name() + "] has reached its EOF position [" + this.readByteAddress + "] and is closed");
    }
  }
  
  public Map<String, Object> getEventMetadata()
  {
    return this.eventMetadataMap;
  }
}
