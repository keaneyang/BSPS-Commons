 package com.bloom.source.lib.directory;
 
 import com.bloom.source.lib.prop.Property;
import com.bloom.common.exc.AdapterException;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
 
 
 
 
 
 
 public class LocalDirectoryWatcher
   extends Watcher
 {
   Logger logger = Logger.getLogger(LocalDirectoryWatcher.class);
   
   WatchService watcher;
   
   WatchKey key;
   
   WatchEvent cast(WatchEvent event)
   {
     return event;
   }
   
   public LocalDirectoryWatcher(Property prop, WatcherCallback callback) throws IOException {
     super(prop, callback);
   }
   
   protected void init() throws AdapterException {
     try {
       this.watcher = FileSystems.getDefault().newWatchService();
       register(this.dir);
     } catch (IOException e) {
       throw new AdapterException("Got exception while initializing WatcherService", e);
     }
   }
   
 
   private void register(Path dir)
     throws IOException
   {
     this.key = dir.register(this.watcher, new WatchEvent.Kind[] { StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY, StandardWatchEventKinds.ENTRY_DELETE });
   }
   
   public void stop() {
     super.stop();
     try {
       this.watcher.close();
       this.logger.debug("Stopped LocalDirectoryWatcher....");
     } catch (IOException e) {
       this.logger.warn("Got exception while closing watcher service");
     }
   }
   
   public void run() {
     for (;;) { 
    	 if (!this.stopCalled)
         try {
           this.key = this.watcher.poll(1000L, TimeUnit.MILLISECONDS);
           if ((this.key != null) && (!this.stopCalled)) {
             for (WatchEvent<?> event : this.key.pollEvents()) {
               WatchEvent.Kind<?> kind = event.kind();
               WatchEvent<Path> ev = cast(event);
               Path name = (Path)ev.context();
               Path resolvedPath = this.dir.resolve(name);
               if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                 onFileCreate(resolvedPath.toFile());
               } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                 onFileModify(resolvedPath.toFile());
               } else if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                 onFileDelete(resolvedPath.toFile());
               } else if ((kind != StandardWatchEventKinds.OVERFLOW) && 
                 (this.logger.isDebugEnabled()))
                 this.logger.debug("Unhandled watcher event received");
             }
             this.key.reset();
           }
         } catch (Exception e) {
           if (this.stopCalled != true)
           {
             this.logger.debug("Got exception while polling DirectoryWatcher service", e); }
         }
     }
     this.logger.debug("Exiting LocalDiretoryWatcher service");
   }
   
   private void foreach(WatchEvent<?> watchEvent) {}
 }

