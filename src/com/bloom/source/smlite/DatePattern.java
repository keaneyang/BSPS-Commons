package com.bloom.source.smlite;

public class DatePattern
  extends Pattern
{
  String[] shortMonths = { "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
  String[] shortDays = { "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun" };
  String[] fullMonth = { "January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December" };
  String[] patternToMatch = { "%month", "%dd", "%d", "%mon", "%z", "%Z", "%yyyy", "%YYYY", "%yy", "%YY", "%H", "%M", "%S", "%sss", "%p", "%E" };
  String[] sampleDates;
  String dateFormat;
  String prefix = "";
  String postfix = "";
  boolean hasDayMarker;
  
  public void init(String format)
  {
    super.init(this.pattern);
    this.expressionType = "NumericExpression";
    buildSampleDateStr(format);
  }
  
  public String[] getListOfPatterns()
  {
    return this.sampleDates;
  }
  
  protected void buildSampleDateStr(String format)
  {
    String tmpDateString = format;
    
    int prefixIdx = tmpDateString.indexOf('%');
    if (prefixIdx != -1) {
      this.prefix = tmpDateString.substring(0, prefixIdx);
    }
    int postfixIdx = tmpDateString.lastIndexOf('%');
    if (postfixIdx != -1)
    {
      int idx = -1;
      for (int xtr = 0; xtr < this.patternToMatch.length; xtr++) {
        if ((idx = tmpDateString.indexOf(this.patternToMatch[xtr], postfixIdx)) != -1)
        {
          int len = this.patternToMatch[xtr].length();
          this.postfix = tmpDateString.substring(idx + len, format.length());
          break;
        }
      }
    }
    this.dateFormat = tmpDateString;
    if (tmpDateString.indexOf("%p") != -1) {
      this.hasDayMarker = true;
    }
    this.dateFormat = this.dateFormat.substring(this.prefix.length(), this.dateFormat.length() - this.postfix.length());
    
    tmpDateString = tmpDateString.replace("%YYYY", "1970");
    tmpDateString = tmpDateString.replace("%YY", "70");
    tmpDateString = tmpDateString.replace("%Y", "1970");
    this.dateFormat = this.dateFormat.replace("%YYYY", "yyyy");
    this.dateFormat = this.dateFormat.replace("%YY", "yy");
    this.dateFormat = this.dateFormat.replace("%Y", "yyyy");
    
    tmpDateString = tmpDateString.replace("%yyyy", "1970");
    tmpDateString = tmpDateString.replace("%yy", "70");
    tmpDateString = tmpDateString.replace("%y", "1970");
    this.dateFormat = this.dateFormat.replace("%yyyy", "yyyy");
    this.dateFormat = this.dateFormat.replace("%yy", "yy");
    this.dateFormat = this.dateFormat.replace("%y", "yyyy");
    if (this.hasDayMarker) {
      this.dateFormat = this.dateFormat.replace("%H", "hh");
    } else {
      this.dateFormat = this.dateFormat.replace("%H", "HH");
    }
    this.dateFormat = this.dateFormat.replace("%M", "mm");
    this.dateFormat = this.dateFormat.replace("%S", "ss");
    this.dateFormat = this.dateFormat.replace("%sss", "SSS");
    if (tmpDateString.indexOf("%mon") != -1)
    {
      this.sampleDates = new String[this.shortMonths.length];
      for (int itr = 0; itr < this.shortMonths.length; itr++) {
        this.sampleDates[itr] = tmpDateString.replace("%mon", this.shortMonths[itr]);
      }
      this.dateFormat = this.dateFormat.replace("%mon", "MMM");
    }
    else if (tmpDateString.indexOf("%month") != -1)
    {
      this.sampleDates = new String[this.fullMonth.length];
      for (int itr = 0; itr < this.fullMonth.length; itr++) {
        this.sampleDates[itr] = tmpDateString.replace("%month", this.fullMonth[itr]);
      }
      this.dateFormat = this.dateFormat.replace("%month", "MMMMM");
    }
    else if (tmpDateString.indexOf("%mm") != -1)
    {
      this.sampleDates = new String[1];
      tmpDateString = tmpDateString.replace("%mm", "01");
      this.sampleDates[0] = tmpDateString;
      this.dateFormat = this.dateFormat.replace("%mm", "MM");
    }
    else if (tmpDateString.indexOf("%m") != -1)
    {
      this.sampleDates = new String[2];
      this.sampleDates[0] = tmpDateString.replace("%m", "1");
      this.sampleDates[1] = tmpDateString.replace("%m", "11");
      this.dateFormat = this.dateFormat.replace("%m", "M");
    }
    else
    {
      this.sampleDates = new String[1];
      this.sampleDates[0] = tmpDateString;
    }
    if (tmpDateString.indexOf("%dd") != -1)
    {
      for (int itr = 0; itr < this.sampleDates.length; itr++) {
        this.sampleDates[itr] = this.sampleDates[itr].replace("%dd", "11");
      }
      this.dateFormat = this.dateFormat.replace("%dd", "dd");
    }
    else if (tmpDateString.indexOf("%d") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 2];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%d", "01");
        tmp[(index++)] = this.sampleDates[itr].replace("%d", "1");
      }
      this.dateFormat = this.dateFormat.replace("%d", "d");
      this.sampleDates = tmp;
    }
    if (tmpDateString.indexOf("%H") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 2];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%H", "1");
        tmp[(index++)] = this.sampleDates[itr].replace("%H", "11");
      }
      this.sampleDates = tmp;
    }
    if (tmpDateString.indexOf("%M") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 2];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%M", "1");
        tmp[(index++)] = this.sampleDates[itr].replace("%M", "11");
      }
      this.sampleDates = tmp;
    }
    if (tmpDateString.indexOf("%S") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 2];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%S", "1");
        tmp[(index++)] = this.sampleDates[itr].replace("%S", "11");
      }
      this.sampleDates = tmp;
    }
    if (tmpDateString.indexOf("%sss") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 3];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%sss", "1");
        tmp[(index++)] = this.sampleDates[itr].replace("%sss", "11");
        tmp[(index++)] = this.sampleDates[itr].replace("%sss", "111");
      }
      this.sampleDates = tmp;
    }
    if (tmpDateString.indexOf("%p") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 4];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%p", "AM");
        tmp[(index++)] = this.sampleDates[itr].replace("%p", "PM");
        tmp[(index++)] = this.sampleDates[itr].replace("%p", "am");
        tmp[(index++)] = this.sampleDates[itr].replace("%p", "pm");
      }
      this.sampleDates = tmp;
      this.dateFormat = this.dateFormat.replace("%p", "a");
    }
    if (tmpDateString.indexOf("%z") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 2];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++)
      {
        tmp[(index++)] = this.sampleDates[itr].replace("%z", "+0000");
        tmp[(index++)] = this.sampleDates[itr].replace("%z", "-0000");
      }
      this.sampleDates = tmp;
      this.dateFormat = this.dateFormat.replace("%z", "Z");
    }
    if (tmpDateString.indexOf("%E") != -1)
    {
      String[] tmp = new String[this.sampleDates.length * 7];
      int index = 0;
      for (int itr = 0; itr < this.sampleDates.length; itr++) {
        for (int dayItr = 0; dayItr < this.shortDays.length; dayItr++) {
          tmp[(index++)] = this.sampleDates[itr].replace("%E", this.shortDays[dayItr]);
        }
      }
      this.sampleDates = tmp;
      
      this.dateFormat = this.dateFormat.replace("%E", "E");
    }
  }
  
  public SMEvent updateEventAttribute(SMEvent event)
  {
    event.isDateEvent = true;
    event.dateFormat = this.dateFormat;
    event.prefixLength = this.prefix.length();
    event.postfixLength = this.postfix.length();
    return event;
  }
}
