 package com.bloom.source.WizardCommons;
 
 
 
 
 public class wizardSQL
 {
   public static String OracleVersionCheck = "SELECT * FROM v$version WHERE banner LIKE 'Oracle%'";
   public static String MSVersionCheck = "SELECT @@version";
   public static String OraclelogMode = "select log_mode from v$database";
 }


