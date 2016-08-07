 package com.bloom.source.cdc.common;
 
 import com.bloom.source.lib.exc.TransactionManagerException;
import com.bloom.common.errors.Error;
 
 
 
 
 
 
 
 
 
 public class CoreException
   extends TransactionManagerException
 {
   private static final long serialVersionUID = -4551551050044860768L;
   Error errorCode;
   
   public CoreException(Error errCode)
   {
     super(errCode.toString());
     this.errorCode = errCode;
     setErrorMessage(errCode.toString());
     setComponentName(null);
   }
   
   public CoreException(Error errCode, String errMsg) {
     super(errCode.toString() + errMsg);
     this.errorCode = errCode;
     setErrorMessage(errCode.toString() + " : " + errMsg);
     setComponentName(null);
   }
   
   public int getErrorCode() {
     return this.errorCode.type;
   }
   
 
   public void setErrorCode(Error errCode)
   {
     this.errorCode = errCode;
   }
 }

