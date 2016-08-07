package com.bloom.source.lib.utils;

import com.bloom.source.lib.prop.Property;
import com.bloom.event.Event;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.log4j.Logger;

public class DirectoryTree
{
  protected String separator;
  private String rootDirectory;
  private String directoryPrefix;
  private String directorySuffix;
  private String currentDirectoryName;
  protected Map<String, Object> directoryCache;
  private Map<String, Field> tokenFieldMap;
  private List<String> childDirectoryList;
  private boolean directoryNameMalformed = false;
  private Logger logger = Logger.getLogger(DirectoryTree.class);
  
  public DirectoryTree(String rootDirectory, Field field, Map<String, Object> properties)
  {
    Property property = new Property(properties);
    this.rootDirectory = rootDirectory;
    this.childDirectoryList = new LinkedList();
    this.directoryCache = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    this.tokenFieldMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    this.tokenFieldMap.put(rootDirectory, field);
    this.directoryPrefix = property.getString("directoryprefix", "");
    this.directorySuffix = property.getString("directorysuffix", "");
    this.separator = FileSystems.getDefault().getSeparator();
  }
  
  public void addChildDirectory(String childDirectory, Field field)
  {
    this.childDirectoryList.add(childDirectory);
    this.tokenFieldMap.put(childDirectory, field);
  }
  
  private String formDirectoryString(Event event)
    throws IOException
  {
    try
    {
      String directoryToBeCreated = null;
      String rootDirectoryName = getValidatedDirectoryTokenValue(((Field)this.tokenFieldMap.get(this.rootDirectory)).get(event));
      directoryToBeCreated = this.directoryPrefix + rootDirectoryName;
      Iterator<String> childDirectoryIterator = this.childDirectoryList.iterator();
      while (childDirectoryIterator.hasNext())
      {
        String childDirectoryName = getValidatedDirectoryTokenValue(((Field)this.tokenFieldMap.get(childDirectoryIterator.next())).get(event));
        directoryToBeCreated = directoryToBeCreated + this.separator + childDirectoryName;
      }
      return directoryToBeCreated + this.directorySuffix;
    }
    catch (IllegalArgumentException|IllegalAccessException e)
    {
      throw new IOException("Unable to form directory string", e);
    }
  }
  
  public boolean createDirectory(Event event)
    throws IOException
  {
    String directoryToBeCreated = formDirectoryString(event);
    if (this.directoryNameMalformed)
    {
      this.logger.warn("Malformed directory " + directoryToBeCreated + ". Discarding event " + event.toString());
      return false;
    }
    this.currentDirectoryName = directoryToBeCreated;
    if (this.directoryCache.get(directoryToBeCreated) == null) {
      return createDirectory(directoryToBeCreated);
    }
    return false;
  }
  
  protected boolean createDirectory(String directoryName)
    throws IOException
  {
    File directory = new File(this.currentDirectoryName);
    this.directoryCache.put(this.currentDirectoryName, directory);
    if (directory.mkdirs()) {
      return true;
    }
    if (directory.exists()) {
      return true;
    }
    throw new IOException("Failure in creating directory " + directory.getAbsolutePath());
  }
  
  public String getCurrentDirectoryName()
  {
    return this.currentDirectoryName;
  }
  
  public boolean isDirectoryNameMalformed()
  {
    if (this.directoryNameMalformed)
    {
      this.directoryNameMalformed = false;
      return true;
    }
    return false;
  }
  
  private String getValidatedDirectoryTokenValue(Object directoryTokenValue)
  {
    String directoryName = null;
    if (directoryTokenValue != null)
    {
      directoryName = directoryTokenValue.toString().trim();
      if (directoryName.isEmpty()) {
        this.directoryNameMalformed = true;
      }
    }
    else
    {
      this.directoryNameMalformed = true;
    }
    return directoryName;
  }
}
