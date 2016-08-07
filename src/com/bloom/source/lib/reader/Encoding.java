package com.bloom.source.lib.reader;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import com.bloom.common.errors.Error;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderMalfunctionError;
import java.nio.charset.CoderResult;
import org.apache.log4j.Logger;

public class Encoding
  extends ReaderBase
{
  private final String DEFAULT_ID = "Default";
  private CharsetDecoder decoder;
  private ByteBuffer byteBuffer;
  CoderResult decodeLoop;
  CharBuffer buffer;
  private boolean recovery = false;
  Logger logger = Logger.getLogger(Encoding.class);
  CharBuffer charBuff;
  boolean leftOutFlag;
  ByteBuffer leftOut;
  private char characterToReplace = '?';
  
  public Encoding(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public Encoding(Property prop)
    throws AdapterException
  {
    super(prop);
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    this.buffer = null;
    String charSet = CharSet();
    if (charSet != null) {
      this.decoder = Charset.forName(charSet).newDecoder();
    } else {
      this.decoder = Charset.forName("UTF-8").newDecoder();
    }
    if (this.logger.isTraceEnabled()) {
      this.logger.trace("Encoding layer has been intialized with charset " + this.decoder.charset().name() + ". System's default charset is " + Charset.defaultCharset().name());
    }
    this.charBuff = CharBuffer.allocate((int)(blockSize() * 1.5D));
    this.leftOut = ByteBuffer.allocate(blockSize());
  }
  
  public Object readBlock()
    throws AdapterException
  {
    if ((this.buffer != null) && (this.recovery))
    {
      this.recovery = false;
      return this.buffer;
    }
    this.byteBuffer = ((ByteBuffer)this.linkedStrategy.readBlock());
    if (this.byteBuffer == null) {
      return null;
    }
    if (this.leftOutFlag)
    {
      this.leftOutFlag = false;
      int limit = this.leftOut.limit();
      if (this.logger.isDebugEnabled()) {
        this.logger.debug("Going to copy left out binary data of size {" + limit + "}");
      }
      int size = this.byteBuffer.limit() + limit;
      
      int additionalBytesRequired = limit - (this.byteBuffer.capacity() - this.byteBuffer.limit());
      if (additionalBytesRequired > 0)
      {
        ByteBuffer tmp = ByteBuffer.allocate(this.byteBuffer.capacity() + additionalBytesRequired);
        tmp.put(this.leftOut);
        tmp.put(this.byteBuffer);
        tmp.flip();
        this.byteBuffer = tmp;
      }
      else
      {
        System.arraycopy(this.byteBuffer.array(), 0, this.byteBuffer.array(), limit, this.byteBuffer.limit());
        System.arraycopy(this.leftOut.array(), 0, this.byteBuffer.array(), 0, limit);
        this.byteBuffer.position(0);
        this.byteBuffer.limit(size);
      }
    }
    return decode(this.byteBuffer);
  }
  
  public Object readBlock(boolean multiEndpointSupport)
    throws AdapterException
  {
    if ((this.buffer != null) && (this.recovery))
    {
      this.recovery = false;
      return this.buffer;
    }
    Object obj = this.linkedStrategy.readBlock();
    if (obj != null) {
      if ((obj instanceof DataPacket))
      {
        DataPacket packet = (DataPacket)obj;
        if (packet != null) {
          packet.data(decode((ByteBuffer)packet.data()));
        }
      }
      else
      {
        obj = new DataPacket(obj, "Default");
      }
    }
    return obj;
  }
  
  private CharBuffer recoveryData(ByteBuffer buffer, CharBuffer charBuffer)
  {
    CharBuffer tmpCharBuffer = CharBuffer.allocate(blockSize());
    tmpCharBuffer.flip();
    int charCount = 0;
    try
    {
      int bytesConverted = charBuffer.toString().getBytes(CharSet()).length;
      int bytesToProcess = buffer.limit() - bytesConverted;
      CharBuffer charBuf = CharBuffer.allocate(blockSize());
      charBuf.limit(0);
      ByteBuffer tmpBuf = ByteBuffer.allocate(bytesToProcess);
      System.arraycopy(buffer.array(), bytesConverted, tmpBuf.array(), 0, bytesToProcess);
      tmpBuf.limit(bytesToProcess);
      while (bytesToProcess > 10)
      {
        tmpBuf.limit(bytesToProcess);
        tmpBuf.position(0);
        charBuf.clear();
        CoderResult result = this.decoder.decode(tmpBuf, charBuf, false);
        charBuf.flip();
        int bytesDecoded = 0;
        if (!result.isError()) {
          break;
        }
        if (result.isMalformed())
        {
          if (charBuf.limit() > 0)
          {
            if (tmpCharBuffer.limit() > 0)
            {
              System.arraycopy(tmpCharBuffer.array(), 0, charBuffer.array(), charBuffer.length(), tmpCharBuffer.limit());
              charBuffer.limit(charBuffer.limit() + tmpCharBuffer.limit());
              charCount = 0;
              tmpCharBuffer.clear();
              tmpCharBuffer.flip();
            }
            System.arraycopy(charBuf.array(), 0, charBuffer.array(), charBuffer.limit(), charBuf.limit());
            charBuffer.limit(charBuffer.limit() + charBuf.limit());
            bytesDecoded = charBuf.toString().getBytes(CharSet()).length;
            bytesToProcess -= charBuf.toString().getBytes(CharSet()).length;
            charBuf.limit(0);
          }
          else
          {
            for (int idx = 0; idx < result.length(); idx++)
            {
              tmpCharBuffer.limit(tmpCharBuffer.limit() + 1);
              tmpCharBuffer.append(this.characterToReplace);
            }
            charCount += result.length();
            bytesToProcess -= result.length();
            bytesDecoded = result.length();
          }
          System.arraycopy(tmpBuf.array(), bytesDecoded, tmpBuf.array(), 0, bytesToProcess);
          tmpBuf.limit(bytesToProcess);
        }
        else
        {
          this.logger.warn("Could not recover data.");
          return null;
        }
      }
      if (bytesToProcess < 10)
      {
        System.arraycopy(tmpBuf.array(), 0, this.leftOut.array(), 0, tmpBuf.limit());
        this.leftOut.limit(tmpBuf.limit());
        this.leftOutFlag = true;
      }
      if (tmpCharBuffer.limit() > 0)
      {
        System.arraycopy(tmpCharBuffer.array(), 0, charBuffer.array(), charBuffer.limit(), tmpCharBuffer.limit());
        charBuffer.limit(charBuffer.limit() + tmpCharBuffer.limit());
      }
      if (charBuf.limit() > 0)
      {
        System.arraycopy(charBuf.array(), 0, charBuffer.array(), charBuffer.limit(), charBuf.limit());
        charBuffer.limit(charBuffer.limit() + charBuf.limit());
      }
    }
    catch (UnsupportedEncodingException e)
    {
      e.printStackTrace();
    }
    return charBuffer;
  }
  
  private synchronized CharBuffer decode(ByteBuffer buffer)
    throws AdapterException
  {
    try
    {
      CoderResult result = null;
      this.charBuff.clear();
      result = this.decoder.decode(buffer, this.charBuff, false);
      this.charBuff.flip();
      if (result.isError())
      {
        if (result.isMalformed())
        {
          this.logger.warn("Decoding failed, will try to recovery.");
          int bytesLost = result.length();
          int bytesConverted = this.charBuff.toString().getBytes(CharSet()).length;
          this.charBuff = recoveryData(buffer, this.charBuff);
          if (this.charBuff != null) {
            return this.charBuff;
          }
        }
        this.logger.warn("Error while decoding the data (charset used: " + CharSet() + ")");
        throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
      }
      return this.charBuff;
    }
    catch (CoderMalfunctionError|UnsupportedEncodingException exp)
    {
      this.logger.warn("Error while decoding the data : " + exp.getMessage());
      throw new AdapterException(Error.UNSUPPORTED_CHARSET_NAME);
    }
  }
  
  public void close()
    throws IOException
  {
    this.linkedStrategy.close();
    this.byteBuffer = null;
    this.charBuff = null;
  }
  
  public int available()
  {
    int len = this.buffer.limit() - this.buffer.position();
    return len;
  }
  
  public int read()
    throws IOException
  {
    if (this.buffer.position() < this.buffer.limit()) {
      return this.buffer.get();
    }
    try
    {
      this.buffer = ((CharBuffer)readBlock());
      if (this.buffer != null) {
        return this.buffer.get();
      }
      return -1;
    }
    catch (AdapterException exp)
    {
      exp.printStackTrace();
    }
    return 0;
  }
  
  public void position(CheckpointDetail attr, boolean pos)
    throws AdapterException
  {
    this.linkedStrategy.position(attr, pos);
    if ((attr != null) && 
      (attr.getRecordBeginOffset() != null)) {
      skip(attr.getRecordBeginOffset().longValue());
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
  
  public long skip(long characterOffset)
  {
    this.recovery = true;
    int charactersInBuffer = 0;
    long charactersToBeSkipped = characterOffset;
    while (characterOffset > 0L) {
      try
      {
        CharBuffer charbuff = (CharBuffer)readBlock();
        if (charbuff != null)
        {
          charactersInBuffer = charbuff.length();
          if (characterOffset > charactersInBuffer)
          {
            characterOffset -= charactersInBuffer;
          }
          else
          {
            charactersInBuffer = (int)(charactersInBuffer - characterOffset);
            if (charactersInBuffer > 0)
            {
              int leftOverToBeCopied = charbuff.length() - (int)characterOffset;
              this.buffer = CharBuffer.allocate(blockSize());
              System.arraycopy(charbuff.array(), (int)characterOffset, this.buffer.array(), 0, leftOverToBeCopied);
              this.buffer.limit(leftOverToBeCopied);
            }
            break;
          }
        }
        else
        {
          if (charactersInBuffer > 0) {
            this.logger.error("Characters to be skipped is [" + charactersToBeSkipped + "] but characters skipped is [" + characterOffset + "]");
          } else {
            this.logger.error("Characters to be skipped is [" + charactersToBeSkipped + "] but characters skipped is [" + charactersInBuffer + "]");
          }
          break;
        }
      }
      catch (AdapterException e)
      {
        e.printStackTrace();
      }
    }
    return characterOffset;
  }
}
