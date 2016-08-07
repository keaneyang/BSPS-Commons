package com.bloom.source.lib.rollingpolicy.util;

import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class RolloverFilenameFormat
{
  private final String filenamePrefix;
  private final String filenameSuffix;
  private long numericalSequence = -1L;
  private DateTimeFormatter dateTimeFormatter;
  private String currentFilename;
  private final String tokenValue;
  private DecimalFormat df;
  private final long fileLimit;
  private final int sequenceStart;
  private final int incrementSequenceBy;
  
  public RolloverFilenameFormat(String pattern)
  {
    this(pattern, -1L, 0, 1, true);
  }
  
  public RolloverFilenameFormat(String pattern, long fileLimit, int sequenceStart, int incrementSequenceBy, boolean addDefaultSequence)
  {
    String fileName = pattern;
    if (fileName.contains("%"))
    {
      int count = countOccurrencesOfToken(fileName, '%');
      int startIndexOfToken = fileName.indexOf('%');
      int endIndexOfToken = fileName.lastIndexOf('%');
      
      String fileSuffix = "";String filePrefix = "";
      if (count != 2) {
        throw new IllegalArgumentException("Failure in initializing writer. Invalid filename " + fileName + " Valid filename tokens are %n% and %sequence%.");
      }
      String token = fileName.substring(startIndexOfToken + 1, endIndexOfToken);
      if (startIndexOfToken == 0)
      {
        fileSuffix = fileName.substring(endIndexOfToken + 1);
      }
      else if (endIndexOfToken == fileName.length())
      {
        filePrefix = fileName.substring(0, startIndexOfToken);
      }
      else
      {
        filePrefix = fileName.substring(0, startIndexOfToken);
        fileSuffix = fileName.substring(endIndexOfToken + 1, fileName.length());
      }
      this.tokenValue = token;
      this.filenamePrefix = filePrefix;
      this.filenameSuffix = fileSuffix;
    }
    else
    {
      String fileExtension = "";
      if (fileName.contains("."))
      {
        int index = fileName.indexOf(".");
        fileExtension = fileName.substring(index, fileName.length());
        fileName = fileName.substring(0, index);
      }
      this.filenamePrefix = (fileName + ".");
      this.filenameSuffix = fileExtension;
      if (addDefaultSequence)
      {
        this.tokenValue = "nn";
      }
      else
      {
        this.tokenValue = null;
        this.currentFilename = (fileName + fileExtension);
      }
    }
    this.fileLimit = fileLimit;
    this.sequenceStart = sequenceStart;
    this.incrementSequenceBy = incrementSequenceBy;
    if ((this.tokenValue != null) && 
      (!this.tokenValue.trim().isEmpty())) {
      if (this.tokenValue.contains("n"))
      {
        int countOfToken = countOccurrencesOfToken(this.tokenValue, 'n');
        String formatValue = "0";
        for (int i = 0; i < countOfToken - 1; i++) {
          formatValue = formatValue + "0";
        }
        this.df = ((DecimalFormat)NumberFormat.getInstance());
        this.df.applyPattern(formatValue);
        this.numericalSequence = sequenceStart;
        this.dateTimeFormatter = null;
      }
      else
      {
        this.dateTimeFormatter = DateTimeFormat.forPattern(this.tokenValue);
        this.numericalSequence = -1L;
      }
    }
  }
  
  public String getNextSequence()
  {
    if (this.numericalSequence != -1L)
    {
      this.currentFilename = (this.filenamePrefix + this.df.format(this.numericalSequence) + this.filenameSuffix);
      this.numericalSequence += this.incrementSequenceBy;
      if (this.numericalSequence == this.fileLimit) {
        this.numericalSequence = this.sequenceStart;
      }
      return this.currentFilename;
    }
    if (this.dateTimeFormatter != null)
    {
      DateTime currentTime = DateTime.now();
      this.currentFilename = (this.filenamePrefix + currentTime.toString(this.dateTimeFormatter) + this.filenameSuffix);
      return this.currentFilename;
    }
    return this.currentFilename;
  }
  
  public String getCurrentFileName()
  {
    return this.currentFilename;
  }
  
  private int countOccurrencesOfToken(String identifier, char token)
  {
    int count = 0;
    for (int i = 0; i < identifier.length(); i++) {
      if (identifier.charAt(i) == token) {
        count++;
      }
    }
    return count;
  }
  
  public static void main(String[] args)
    throws Exception
  {
    RolloverFilenameFormat filenameFormat = new RolloverFilenameFormat("/tmp/abc_%n%.log.gz", 2L, 0, 1, true);
    String s = filenameFormat.getNextSequence();
    System.out.println(s);
    s = filenameFormat.getNextSequence();
    System.out.println(s);
    s = filenameFormat.getNextSequence();
    System.out.println(s);
  }
}
