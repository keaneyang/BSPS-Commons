package com.bloom.source.lib.exc;

import com.bloom.common.errors.Error;

public abstract class TransactionManagerException
  extends Exception
{
  String className;
  String methodName;
  String fileName;
  int lineNumber;
  String componentName;
  String errorMessage;
  Error genericErrorCode;
  private static final long serialVersionUID = -2685486125213211132L;
  
  public TransactionManagerException(String msg)
  {
    super(msg);
    StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
    setClassName(frame.getClassName());
    setMethodName(frame.getMethodName());
    setFileName(frame.getFileName());
    setLineNumber(frame.getLineNumber());
  }
  
  public TransactionManagerException(String msg, Exception e)
  {
    super(msg, e);
    
    StackTraceElement frame = Thread.currentThread().getStackTrace()[2];
    setClassName(frame.getClassName());
    setMethodName(frame.getMethodName());
    setFileName(frame.getFileName());
    setLineNumber(frame.getLineNumber());
  }
  
  public String getClassName()
  {
    return this.className;
  }
  
  public void setClassName(String className)
  {
    this.className = className;
  }
  
  public String getMethodName()
  {
    return this.methodName;
  }
  
  public void setMethodName(String methodName)
  {
    this.methodName = methodName;
  }
  
  public String getFileName()
  {
    return this.fileName;
  }
  
  public void setFileName(String fileName)
  {
    this.fileName = fileName;
  }
  
  public int getLineNumber()
  {
    return this.lineNumber;
  }
  
  public void setLineNumber(int lineNumber)
  {
    this.lineNumber = lineNumber;
  }
  
  public abstract int getErrorCode();
  
  public void setGenericErrorCode(Error genErrorCode)
  {
    this.genericErrorCode = genErrorCode;
  }
  
  public void setErrorMessage(String errMessage)
  {
    this.errorMessage = errMessage;
  }
  
  public String getErrorMessage()
  {
    return this.errorMessage;
  }
  
  public String getComponentName()
  {
    return this.componentName;
  }
  
  public void setComponentName(String componentName)
  {
    this.componentName = componentName;
  }
}
