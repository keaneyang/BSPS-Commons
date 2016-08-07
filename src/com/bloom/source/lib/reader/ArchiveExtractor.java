package com.bloom.source.lib.reader;

import java.io.IOException;
import java.io.InputStream;

import com.bloom.source.lib.prop.Property;
import com.bloom.common.errors.Error;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;

import com.bloom.common.exc.AdapterException;

public class ArchiveExtractor
  extends StreamReader
{
  private ArchiveInputStream archiveStream;
  
  protected ArchiveExtractor(ReaderBase link)
    throws AdapterException
  {
    super(link);
  }
  
  public void init()
    throws AdapterException
  {
    super.init();
    try
    {
      String type = this.linkedStrategy.property().getString(Property.ARCHIVE_TYPE, "");
      this.archiveStream = new ArchiveStreamFactory().createArchiveInputStream(type, new Reader(this.linkedStrategy));
      this.dataSource = getStream(this.archiveStream);
    }
    catch (ArchiveException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
  
  public Object readBlock()
    throws AdapterException
  {
    try
    {
      if (this.dataSource.available() == 0) {
        this.dataSource = getStream(this.archiveStream);
      }
      return super.readBlock();
    }
    catch (IOException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
  
  private InputStream getStream(ArchiveInputStream stream)
    throws AdapterException
  {
    try
    {
      if (this.archiveStream != null)
      {
        if (name() != null) {
          onClose(name());
        }
        ArchiveEntry entry = stream.getNextEntry();
        if (entry != null)
        {
          if (entry.isDirectory()) {
            return getStream(stream);
          }
          onOpen(entry.getName());
          name(entry.getName());
          return stream;
        }
      }
      return null;
    }
    catch (IOException e)
    {
      throw new AdapterException(Error.GENERIC_IO_EXCEPTION, e);
    }
  }
}
