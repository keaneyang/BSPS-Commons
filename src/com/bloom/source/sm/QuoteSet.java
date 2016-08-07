 package com.bloom.source.sm;
 
 public class QuoteSet { private char begin;
   private char end;
   private QuoteState currState;
   private String quoteAsString;
   
   public static enum QuoteState { NOT_MATCHED, 
     BEGIN, 
     END;
     
 
 
     private QuoteState() {}
   }
   
 
   public String getQuoteAsString()
   {
     return this.quoteAsString;
   }
   
   public QuoteSet(String str) {
     this.quoteAsString = str;
     if (this.quoteAsString.length() == 1) {
       this.begin = this.quoteAsString.charAt(0);
       this.end = this.quoteAsString.charAt(0);
     } else {
       this.begin = this.quoteAsString.charAt(0);
       this.end = this.quoteAsString.charAt(1);
     }
     this.currState = QuoteState.NOT_MATCHED;
   }
   
   public QuoteState match(char inputChar)
   {
     if (this.currState == QuoteState.END) {
       this.currState = QuoteState.NOT_MATCHED;
     }
     
     if (this.currState == QuoteState.NOT_MATCHED) {
       if (this.begin == inputChar) {
         this.currState = QuoteState.BEGIN;
       }
     } else if ((this.currState == QuoteState.BEGIN) && 
       (this.end == inputChar)) {
       this.currState = QuoteState.END;
     }
     
     return this.currState;
   }
   
   public void reset() { this.currState = QuoteState.NOT_MATCHED; }
   
   public String toString()
   {
     return this.quoteAsString;
   }
 }

