 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.source.lib.utils.Compare;
import com.bloom.source.lib.utils.DefaultFileComparator;
import com.bloom.common.exc.AdapterException;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 
 
 
 
 public class Directory
 {
   static Logger logger = Logger.getLogger(Directory.class);
   private File folder;
   protected PriorityBlockingQueue<Object> fileList;
   Object currentFile = null;
   boolean finished = false;
   Compare[] comp = null;
   PathMatcher matcher = null;
   
   Thread WatcherThread;
   
   static int threadCount;
   
   Comparator<Object> comparator;
   
 
   public Directory(Comparator<Object> comparator)
   {
     this.comparator = comparator;
     init();
   }
   
 
 
 
 
 
 
 
 
 
   public Directory(Property prop, Comparator<Object> comparator)
     throws IOException, InterruptedException, AdapterException
   {
     this.comparator = comparator;
     init();
     
     String folderName = prop.directory;
     if ((folderName == null) || (folderName == "")) {
       folderName = new File(new File(".").getAbsolutePath()).getCanonicalPath();
     }
     this.folder = new File(folderName);
     
 
 
 
     if (!this.folder.exists()) {
       throw new IOException(folderName + " is not present");
     }
     
 
 
 
 
     if (!this.folder.isDirectory())
       throw new IOException(folderName + " is not a directory");
     if (!this.folder.canRead()) {
       throw new AdapterException(this.folder.getAbsolutePath() + " doesn't have read permission. Please ensure it has proper access rights.");
     }
     if (logger.isInfoEnabled()) { logger.info("Files are in the folder " + this.folder.getCanonicalPath());
     }
     java.nio.file.Path dir = Paths.get(folderName, new String[0]);
     File[] files = this.folder.listFiles();
     if ((files != null) && (files.length > 10000)) {
       logger.warn("Specified directory " + this.folder.getCanonicalPath() + " has " + files.length + " files. FileReader initialization may take time");
     }
     else if ((files != null) && (files.length <= 1)) {
       logger.warn("Specified directory " + this.folder.getCanonicalPath() + " has " + files.length + " files. No event would be produced until a file " + "matching the specified wildcard " + prop.wildcard + " is added to the directory. Please ensure specified directory and wildcard" + " are correct");
     }
     
     matchWildCard(files, prop);
     this.WatcherThread = new Thread(new WatcherDirectory(dir, Boolean.valueOf(prop.registerRecursively), prop));
     this.WatcherThread.setName("WatcherThread" + threadCount++);
     this.WatcherThread.start();
   }
   
   private void init()
   {
     if (this.comparator == null) {
       this.comparator = new DefaultFileComparator();
     }
     this.fileList = new PriorityBlockingQueue(10, this.comparator);
   }
   
 
 
 
 
 
 
 
 
 
 
 
   public void matchWildCard(File[] files, Property prop)
     throws IOException
   {
     if (prop.wildcard != null) {
       this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + prop.wildcard);
     }
     
 
 
 
     for (File file : files) {
       if ((this.matcher == null) || (this.matcher.matches(file.toPath().getFileName()))) {
         this.fileList.add(file);
       }
     }
     
     if ((files.length > 1) && (this.fileList.isEmpty())) {
       logger.warn("Specified directory " + this.folder.getCanonicalPath() + " has " + files.length + " files but specified wildcard " + prop.wildcard + " didn't match any files present in the directory. No event would be produced until a file " + "matching the specified wildcard " + prop.wildcard + " is added to the directory. Please ensure specified directory and wildcard" + " are correct");
     }
   }
   
 
 
 
 
   public Object getCurrentFile()
   {
     return this.currentFile;
   }
   
 
 
 
 
   public int getFileListSize()
   {
     return this.fileList.size();
   }
   
 
 
   public Object getNextFile()
   {
     if (!this.fileList.isEmpty()) {
       this.currentFile = this.fileList.poll();
       return this.currentFile;
     }
     return null;
   }
   
 
 
 
 
 
 
 
 
 
 
   public Object getFile(String fileName)
     throws AdapterException
   {
     Object[] list = new Object[this.fileList.size()];
     list = this.fileList.toArray();
     String name = null;
     for (int idx = 0; idx < list.length; idx++) {
       Object file = list[idx];
       if ((file instanceof File)) {
         name = ((File)file).getName();
       }
       else if ((file instanceof org.apache.hadoop.fs.Path)) {
         name = ((org.apache.hadoop.fs.Path)file).getName();
       } else {
         throw new AdapterException("Unsupported file instance");
       }
       if (name.equals(fileName)) {
         this.currentFile = list[idx];
         this.fileList.remove(this.currentFile);
         for (int itr = 0; itr < idx; itr++)
           this.fileList.remove();
         return this.currentFile;
       }
     }
     return null;
   }
   
 
 
   public boolean isLastFile()
   {
     int filesCount = this.fileList.size();
     if (filesCount == 0)
       return true;
     return false;
   }
   
 
 
   public void stopThread()
   {
     this.finished = true;
     while (this.WatcherThread.getState() != Thread.State.TERMINATED) {
       try {
         Thread.sleep(100L);
       } catch (InterruptedException e) {
         logger.debug("Watcher Directory close interrupted");
       }
     }
   }
   
 
   private class WatcherDirectory
     implements Runnable
   {
     private final WatchService watcher;
     
     private final Map<WatchKey, java.nio.file.Path> keys;
     
     private final boolean nested;
     
     private boolean trace = false;
     PathMatcher matcher = null;
     List<FileTime> tmpFileList = new LinkedList();
     int f = 0;
     Property prop = null;
     
 
 
     WatchEvent cast(WatchEvent event)
     {
       return event;
     }
     
 
 
 
 
 
 
 
 
 
 
     public WatcherDirectory(java.nio.file.Path dir, Boolean nested, Property prop)
       throws IOException
     {
       this.watcher = FileSystems.getDefault().newWatchService();
       this.keys = new HashMap();
       this.nested = nested.booleanValue();
       this.prop = prop;
       
 
 
 
       if (prop.wildcard != null) {
         this.matcher = FileSystems.getDefault().getPathMatcher("glob:" + prop.wildcard);
       }
       
 
 
 
       if (nested.booleanValue()) {
         registerAll(dir);
       } else {
         register(dir);
       }
       
 
 
 
       this.trace = true;
       java.nio.file.Path archiveDir;
       if (prop.archivedir != null) {
         archiveDir = Paths.get(prop.archivedir, new String[0]);
         if (!archiveDir.equals(dir)) {
           registerAll(archiveDir);
         }
       }
     }
     
 
 
 
 
 
 
     public void run()
     {
       while (!Directory.this.finished) {
         int flag = 0;
         
 
         WatchKey key;
         
 
         try
         {
           key = this.watcher.poll(this.prop.eofdelay, TimeUnit.MILLISECONDS);
         } catch (InterruptedException x) {
           return;
         }
         
         java.nio.file.Path dir = (java.nio.file.Path)this.keys.get(key);
         
         if (dir != null)
         {
 
 
           java.nio.file.Path child = null;
           
 
 
 
 
           for (WatchEvent<?> event : key.pollEvents()) {
             WatchEvent.Kind<?> kind = event.kind();
             
 
 
 
             if (kind != StandardWatchEventKinds.OVERFLOW)
             {
 
 
               WatchEvent<java.nio.file.Path> ev = cast(event);
               
 
 
 
               java.nio.file.Path name = (java.nio.file.Path)ev.context();
               
 
 
 
               child = dir.resolve(name);
               
               if (Directory.logger.isDebugEnabled()) {
                 Directory.logger.debug("Event " + event.kind() + "Name " + child);
               }
               
 
 
               if ((this.nested) && (kind == StandardWatchEventKinds.ENTRY_CREATE)) {
                 try {
                   if (Files.isDirectory(child, new LinkOption[] { LinkOption.NOFOLLOW_LINKS })) {
                     registerAll(child);
                   }
                 }
                 catch (IOException x) {}
               }
               
 
               FileTime ctime = null;
               if (this.prop.archivedir != null)
               {
 
 
                 try
                 {
 
 
                   if (Files.isDirectory(child, new LinkOption[0])) {
                     File file = null;
                     file = checkFolder(child);
                     if ((this.matcher == null) || (this.matcher.matches(file.toPath().getFileName()))) {
                       BasicFileAttributes attributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class, new LinkOption[0]);
                       ctime = attributes.creationTime();
                       child = file.toPath();
 
 
                     }
                     
 
 
                   }
                   else if ((this.matcher == null) || (this.matcher.matches(child.getFileName()))) {
                     BasicFileAttributes attributes = Files.readAttributes(child, BasicFileAttributes.class, new LinkOption[0]);
                     ctime = attributes.creationTime();
                   }
                   
                 }
                 catch (IOException e)
                 {
                   Directory.logger.error(e);
                 }
               }
               
               File file = child.toFile();
               
 
 
 
               if ((this.matcher == null) || (this.matcher.matches(file.toPath().getFileName()))) {
                 if (this.prop.archivedir != null) {
                   this.tmpFileList.add(ctime);
                   try {
                     if (this.f != 0)
                       flag = compareFile(file, flag);
                   } catch (IOException|InterruptedException e) {
                     Directory.logger.error(e);
                   }
                 }
                 if ((flag == 0) && (
                   (file.isFile()) || (Files.isSymbolicLink(child)))) {
                   Directory.this.fileList.add(file);
                   if (Directory.logger.isTraceEnabled()) {
                     Directory.logger.trace("A new file " + file.getName() + " is added to the directory");
                   }
                 }
                 if (this.prop.archivedir != null) {
                   this.f = 1;
                 }
               }
             }
           }
           
 
           boolean valid = key.reset();
           if (!valid) {
             this.keys.remove(key);
             
 
 
 
             if (this.keys.isEmpty()) {}
           }
         }
       }
       
       try
       {
         this.watcher.close();
       } catch (IOException e) {
         e.printStackTrace();
       }
     }
     
 
 
 
 
 
 
     public File checkFolder(java.nio.file.Path child)
     {
       File validFile = null;
       File nestedFolder = child.toFile();
       File[] fileList = nestedFolder.listFiles();
       File[] arr$ = fileList;int len$ = arr$.length;int i$ = 0; if (i$ < len$) { File file = arr$[i$];
         if (file.isDirectory()) {
           return checkFolder(file.toPath());
         }
         
         return file;
       }
       
 
 
       return validFile;
     }
     
 
 
 
 
 
 
 
 
 
 
 
 
 
 
     public int compareFile(File file, int flag)
       throws IOException, InterruptedException
     {
       List<File> tempFileList = null;
       File tempFile = null;
       java.nio.file.Path tempPath = null;
       FileTime tmp = null;FileTime cur = null;
       tempFileList = new LinkedList();
       tempFileList.add(file);
       
 
 
 
       if (!tempFileList.isEmpty()) {
         tempFile = (File)tempFileList.remove(0);
         cur = (FileTime)this.tmpFileList.remove(0);
         tempPath = tempFile.toPath();
         
 
 
 
         BasicFileAttributes attributes = Files.readAttributes(tempPath, BasicFileAttributes.class, new LinkOption[0]);
         tmp = attributes.creationTime();
         
 
 
 
         if (tmp.equals(cur)) {
           if (Directory.logger.isInfoEnabled()) Directory.logger.info("Same file is accessed again");
           flag = 1;
 
         }
         else
         {
           Directory.this.fileList.add(tempFile);
           flag = 1;
 
         }
         
 
       }
       else if ((Directory.this.fileList.isEmpty()) && 
         (Directory.logger.isInfoEnabled())) { Directory.logger.info("No files for further processing");
       }
       
 
       return flag;
     }
     
 
 
 
 
 
 
     private void register(java.nio.file.Path dir)
       throws IOException
     {
       WatchKey key = dir.register(this.watcher, new WatchEvent.Kind[] { StandardWatchEventKinds.ENTRY_CREATE });
       if (this.trace) {
         java.nio.file.Path prev = (java.nio.file.Path)this.keys.get(key);
         if (prev == null) {
           if (Directory.logger.isInfoEnabled()) Directory.logger.info("Register the directory " + dir);
         }
         else if ((!dir.equals(prev)) && 
           (Directory.logger.isInfoEnabled())) { Directory.logger.info("update the registry " + prev + " " + dir);
         }
       }
       
       this.keys.put(key, dir);
     }
     
 
 
 
 
 
     private void registerAll(java.nio.file.Path start)
       throws IOException
     {
       Files.walkFileTree(start, new SimpleFileVisitor()
       {
         public FileVisitResult preVisitDirectory(java.nio.file.Path dir, BasicFileAttributes attrs) throws IOException {
           Directory.WatcherDirectory.this.register(dir);
           return FileVisitResult.CONTINUE;
         }
       });
     }
   }
 }

