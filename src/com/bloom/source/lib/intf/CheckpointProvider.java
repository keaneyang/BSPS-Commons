package com.bloom.source.lib.intf;

import com.bloom.recovery.CheckpointDetail;
import com.bloom.recovery.Position;

public abstract interface CheckpointProvider
{
  public abstract CheckpointDetail getCheckpointDetail();
  
  public abstract Position getPositionDetail();
}
