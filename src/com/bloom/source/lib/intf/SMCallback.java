package com.bloom.source.lib.intf;

import com.bloom.source.smlite.ColumnEvent;
import com.bloom.source.smlite.CommentEvent;
import com.bloom.source.smlite.EndOfBlockEvent;
import com.bloom.source.smlite.EscapeEvent;
import com.bloom.source.smlite.NVPEvent;
import com.bloom.source.smlite.QuoteBeginEvent;
import com.bloom.source.smlite.QuoteEndEvent;
import com.bloom.source.smlite.QuoteEvent;
import com.bloom.source.smlite.ResetEvent;
import com.bloom.source.smlite.RowBeginEvent;
import com.bloom.source.smlite.RowEndEvent;
import com.bloom.source.smlite.RowEvent;
import com.bloom.source.smlite.SMEvent;
import com.bloom.source.smlite.TimeStampEvent;

public abstract interface SMCallback
{
  public abstract boolean onEvent(SMEvent paramSMEvent);
  
  public abstract void onEvent(ColumnEvent paramColumnEvent);
  
  public abstract void onEvent(RowEvent paramRowEvent);
  
  public abstract void onEvent(QuoteEvent paramQuoteEvent);
  
  public abstract void onEvent(QuoteBeginEvent paramQuoteBeginEvent);
  
  public abstract void onEvent(QuoteEndEvent paramQuoteEndEvent);
  
  public abstract void onEvent(ResetEvent paramResetEvent);
  
  public abstract void onEvent(RowBeginEvent paramRowBeginEvent);
  
  public abstract void onEvent(RowEndEvent paramRowEndEvent);
  
  public abstract void onEvent(TimeStampEvent paramTimeStampEvent);
  
  public abstract void onEvent(EndOfBlockEvent paramEndOfBlockEvent);
  
  public abstract void onEvent(EscapeEvent paramEscapeEvent);
  
  public abstract void onEvent(CommentEvent paramCommentEvent);
  
  public abstract void onEvent(NVPEvent paramNVPEvent);
}

