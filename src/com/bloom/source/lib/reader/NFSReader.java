package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.BasicFileAttributes;
import org.apache.log4j.Logger;

public class NFSReader
  extends FileReader
{
  Logger logger = Logger.getLogger(NFSReader.class);
  protected long fileSize;
  protected int bytesToRead;
  protected long lastFileSize;
  boolean checkForNullByte = false;
  int nfsReadDelay = 50;
  
  public NFSReader(Property prop)
    throws AdapterException
  {
    super(prop);
    this.checkForNullByte = prop.getBoolean("checkfornullbyte", false);
    this.nfsReadDelay = prop.getInt("nfsreaddelay", 50);
  }
  
  boolean flag = true;
  long prevBytesConsumed;
  int lastBytesRead;
  String curFile;
  String tmpFile;
  FileOutputStream tmpStream;
  
  public Object readBlock()
    throws AdapterException
  {
    if (this.inputStream != null)
    {
      int bytes;
      for (;;)
      {
        this.fileChanged = false;
        if ((this.bytesConsumed >= this.fileSize) || (this.lastBytesRead == -1)) {
          try
          {
            BasicFileAttributes attributes = Files.readAttributes(((File)this.currentFile).toPath(), BasicFileAttributes.class, new LinkOption[0]);
            if (!this.fileKey.equals(attributes.fileKey().toString()))
            {
              this.fileChanged = true;
            }
            else
            {
              this.fileSize = attributes.size();
              if (this.lastFileSize != this.fileSize)
              {
                this.flag = true;
                this.lastFileSize = this.fileSize;
                try
                {
                  Thread.sleep(this.nfsReadDelay);
                }
                catch (InterruptedException e) {}
                if (this.logger.isDebugEnabled()) {
                  this.logger.debug("FileSize {" + this.fileSize + "} BytesConsumed {" + this.bytesConsumed + "} {" + name() + "}");
                }
              }
              else if (this.flag)
              {
                this.logger.debug("Waiting for more data {" + name() + "}");
                this.flag = false;
              }
            }
          }
          catch (IOException e)
          {
            this.logger.debug("Got exception while getting file attribute", e);
            this.fileChanged = true;
          }
        }
        this.bytesToRead = ((int)(this.fileSize - this.bytesConsumed));
        this.bytesToRead = (this.bytesToRead > this.blockSize ? this.blockSize : this.bytesToRead);
        bytes = 0;
        if (this.bytesToRead < 0) {
          this.logger.debug("Negative BytesToReader");
        }
        if ((!this.fileChanged) && (this.bytesToRead > 0)) {
          try
          {
            this.buffer.clear();
            if (this.prevBytesConsumed != this.bytesConsumed)
            {
              this.logger.debug("Trying to read {" + this.bytesToRead + "} from {" + name() + "} Key {" + this.fileKey + "} bytesConsumed {" + this.bytesConsumed + "}");
              this.prevBytesConsumed = this.bytesConsumed;
            }
            bytes = this.inputStream.read(this.buffer.array(), 0, this.bytesToRead);
            this.lastBytesRead = bytes;
            if (bytes > 0) {
              this.logger.debug("Read {" + bytes + "} from {" + name() + "} Key {" + this.fileKey + "} bytesConsumed {" + this.bytesConsumed + "}");
            }
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
            this.fileChanged = true;
            this.logger.error("File : {" + this.name + "} FileReader IOException : {" + exp.getMessage() + "}");
            throw new AdapterException(Error.GENERIC_IO_EXCEPTION, exp);
          }
        }
        if (bytes > 0) {
          break;
        }
        if (!isEOFReached()) {
          return null;
        }
        this.logger.debug("Rolled over to next file {" + name() + "} key {" + this.fileKey + "}");
      }
      this.logger.debug("File {" + name() + "} Key {" + this.fileKey + "} Bytes read {" + bytes + "} bytesConsumed so far {" + this.bytesConsumed + "} fileSize {" + this.fileSize + "}");
      if (this.logger.isTraceEnabled()) {
        this.logger.trace(this.bytesRead + " bytes have been read from file " + name() + " in the directory " + this.prop.directory);
      }
      if (this.checkForNullByte)
      {
        byte[] buf = this.buffer.array();
        for (int itr = 0; itr < bytes; itr++) {
          if (buf[itr] == 0) {
            try
            {
              this.logger.warn("Seen NULL byte while reading {" + name() + "} File offset {" + (this.bytesRead + itr) + "} block {" + bytes + "}");
              if ((this.curFile == null) || (!this.curFile.equals(((File)this.currentFile).getName())))
              {
                this.curFile = ((File)this.currentFile).getName();
                this.tmpFile = ("/tmp/" + ((File)this.currentFile).getName());
                this.logger.debug("Creating tmp file {" + this.tmpFile + "}");
                if (this.tmpStream != null) {
                  this.tmpStream.close();
                }
                this.tmpStream = new FileOutputStream(this.tmpFile);
              }
              this.tmpStream.write(this.buffer.array(), 0, bytes);
              this.tmpStream.flush();
            }
            catch (IOException exp)
            {
              this.logger.debug("Got exception while writing NULL block in to /tmp directory");
            }
          }
        }
      }
      this.buffer.limit(bytes);
      this.bytesConsumed += bytes;
      this.bytesRead += bytes;
      this.recoveryCheckpoint.setBytesRead(this.bytesRead);
      return this.buffer;
    }
    position(null, false);
    return null;
  }
}
