 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.reader.Reader;
import com.bloom.source.lib.utils.DefaultFileComparator;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.PriorityBlockingQueue;
import org.apache.log4j.Logger;
 
 public abstract class Watcher implements Runnable
 {
   Logger logger = Logger.getLogger(Watcher.class);
   
   WatcherCallback callback;
   Thread watcherThread;
   PathMatcher matcher;
   Path dir;
   boolean stopCalled;
   Property prop;
   protected int maxFilesToFetch = -1;
   boolean recoveryMode;
   boolean isMultiFileBank;
   
   public Watcher(Property prop, WatcherCallback callback) throws IOException
   {
     this.callback = callback;
     this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + prop.wildcard);
     this.recoveryMode = prop.getBoolean(Reader.RECOVERY_MODE, false);
     File directory = new File(prop.directory);
     
 
 
 
     if (!directory.exists()) {
       throw new IOException("Directory [" + prop.directory + "] is not present");
     }
     this.dir = Paths.get(prop.directory, new String[0]);
     this.prop = prop;
     this.isMultiFileBank = (callback instanceof MultiFileBank);
   }
   
 
 
   protected abstract void init()
     throws AdapterException;
   
 
 
   public void invalidateCacheFor(String fileName) {}
   
 
   protected File[] getFiles()
     throws AdapterException
   {
     PriorityBlockingQueue<File> fileList = new PriorityBlockingQueue(10, new DefaultFileComparator());
     try
     {
       int fileCnt = 0;
       DirectoryStream<Path> listOfPaths = Files.newDirectoryStream(this.dir, new WildcardFilter(this.prop.wildcard));
       for (Path path : listOfPaths) {
         if ((this.maxFilesToFetch > 0) && (fileCnt >= this.maxFilesToFetch)) {
           break;
         }
         fileCnt++;
         fileList.add(path.toFile());
       }
       listOfPaths.close();
     } catch (IOException e) {
       String msg = "Got IOException while creating DirectoryStream for {" + this.dir.toString() + "}";
       this.logger.warn(msg);
       throw new AdapterException(msg, e);
     }
     
     ArrayList<File> tmpFileList = new ArrayList();
     
     int fileCnt = fileList.size();
     for (int itr = 0; itr < fileCnt; itr++) {
       File file = (File)fileList.poll();
       if (file != null)
         tmpFileList.add(file);
     }
     return (File[])tmpFileList.toArray(new File[tmpFileList.size()]);
   }
   
 
   public void start()
     throws AdapterException
   {
     publishCurrentStatus();
     this.watcherThread = new Thread(this);
     this.watcherThread.setName("WatcherThread-" + this.prop.directory + ":" + this.prop.wildcard);
     this.watcherThread.start();
   }
   
 
   private void publishCurrentStatus()
     throws AdapterException
   {
     File[] fileList = getFiles();
     if ((this.recoveryMode) || (!this.prop.positionByEOF))
     {
 
 
 
 
 
 
 
       for (int itr = 0; itr < fileList.length; itr++) {
         onFileCreate(fileList[itr]);
       }
       
     } else if (!this.isMultiFileBank)
     {
 
 
 
 
 
 
 
 
 
 
 
       if (fileList.length > 0) {
         onFileCreate(fileList[(fileList.length - 1)]);
       }
     }
   }
   
 
 
 
   public void stop()
   {
     this.stopCalled = true;
     if (this.logger.isDebugEnabled())
       this.logger.debug("Signaled wather-thread to stop");
     this.watcherThread.interrupt();
   }
   
 
 
 
   protected boolean checkForWildcardMatch(File file)
   {
     if (this.matcher.matches(file.toPath().getFileName()))
       return true;
     return false;
   }
   
   protected void onFileCreate(File file) {
     if (checkForWildcardMatch(file))
       this.callback.onFileCreate(file);
   }
   
   protected void onFileDelete(File file) {
     if (checkForWildcardMatch(file))
       this.callback.onFileDelete(file);
   }
   
   protected void onFileDelete(String fileKey, String fileName) {
     this.callback.onFileDelete(fileKey, fileName);
   }
   
   protected void onFileModify(File file) {
     if (checkForWildcardMatch(file))
       this.callback.onFileModify(file);
   }
   
   class WildcardFilter implements DirectoryStream.Filter<Path> {
     PathMatcher matcher;
     
     public WildcardFilter(String wildcard) { this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + Watcher.this.prop.wildcard); }
     
 
     public boolean accept(Path entry)
     {
       return this.matcher.matches(entry.getFileName());
     }
   }
 }

