package com.bloom.source.lib.rollingpolicy.util;

import com.bloom.source.lib.rollingpolicy.outputstream.EventCountAndTimeRolloverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.EventCountRollOverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.FilelengthRolloverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.RollOverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.TimeIntervalRollOverOutputStream;
import com.bloom.source.lib.rollingpolicy.outputstream.RollOverOutputStream.OutputStreamBuilder;
import com.bloom.source.lib.rollingpolicy.property.RollOverProperty;

import java.io.IOException;

public class RollOverOutputStreamFactory
{
  public static RollOverOutputStream getRollOverOutputStream(RollOverProperty rollOverProperty, RollOverOutputStream.OutputStreamBuilder outputStreamBuilder, RolloverFilenameFormat filenameFormat)
    throws IOException
  {
    if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("EventCountRollingPolicy")) {
      return new EventCountRollOverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getEventCount(), rollOverProperty.hasFormatterGotHeader());
    }
    if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("FileSizeRollingPolicy")) {
      return new FilelengthRolloverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getFileSize());
    }
    if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("TimeIntervalRollingPolicy")) {
      return new TimeIntervalRollOverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getTimeInterval(), rollOverProperty.hasFormatterGotHeader());
    }
    if (rollOverProperty.getRollingPolicyName().equalsIgnoreCase("EventCountAndTimeRollingPolicy")) {
      return new EventCountAndTimeRolloverOutputStream(outputStreamBuilder, filenameFormat, rollOverProperty.getEventCount(), rollOverProperty.getTimeInterval());
    }
    return new RollOverOutputStream(outputStreamBuilder, filenameFormat) {};
  }
}
