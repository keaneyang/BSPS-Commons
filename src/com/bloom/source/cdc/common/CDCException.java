 package com.bloom.source.cdc.common;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.common.errors.Error;
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 
 public class CDCException
   extends TransactionManagerException
 {
   private static final long serialVersionUID = -4551551050044860768L;
   Error errorCode;
   
   public CDCException(Error errCode)
   {
     super("WA-" + errCode.toString());
     this.errorCode = errCode;
     setErrorMessage("WA-" + errCode.toString());
     setComponentName("CDCReader");
   }
   
   public CDCException(Error errCode, Exception e) {
     super("WA-" + errCode.toString() + ". Cause: " + e.getMessage(), e);
     this.errorCode = errCode;
     if (e.getMessage() != null) {
       setErrorMessage("WA-" + errCode.toString() + ". Cause: " + e.getMessage());
     } else {
       setErrorMessage("WA-" + errCode.toString());
     }
     setComponentName("CDCReader");
   }
   
   public CDCException(Error errCode, Exception e, String addMsg) {
     super("WA-" + errCode.toString() + " " + addMsg + " . Cause: " + e.getMessage(), e);
     this.errorCode = errCode;
     if (e.getMessage() != null) {
       setErrorMessage("WA-" + errCode.toString() + " " + addMsg + " . Cause: " + e.getMessage());
     } else {
       setErrorMessage("WA-" + errCode.toString() + " " + addMsg);
     }
     setComponentName("CDCReader");
   }
   
   public CDCException(Error errCode, String errMsg) { super("WA-" + errCode.toString() + ". Cause: " + errMsg);
     this.errorCode = errCode;
     if (errMsg != null) {
       setErrorMessage("WA-" + errCode.toString() + ". Cause: " + errMsg);
     } else
       setErrorMessage("WA-" + errCode.toString());
     setComponentName("CDCReader");
   }
   
   public Error returnErrorCode() {
     return this.errorCode;
   }
   
   public int getErrorCode() {
     return this.errorCode.type;
   }
   
   public void setErrorCode(Error errCode) {
     this.errorCode = errCode;
   }
 }

