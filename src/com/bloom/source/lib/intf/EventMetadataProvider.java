package com.bloom.source.lib.intf;

import java.util.Map;

public abstract interface EventMetadataProvider
{
  public abstract Map<String, Object> getEventMetadata();
}

