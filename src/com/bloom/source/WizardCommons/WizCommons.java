package com.bloom.source.WizardCommons;

import com.bloom.DBCommons.DatabaseCommon;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class WizCommons
{
  static Connection dbConnection = null;
  DatabaseCommon newCom;
  
  public WizCommons() {}
  
  public WizCommons(Connection conn)
  {
    dbConnection = conn;
    this.newCom = new DatabaseCommon(dbConnection);
  }
  
  public String getTables(String fulltableName, String dbType)
    throws Exception
  {
    List<String> tables = this.newCom.getTables(fulltableName, dbType);
    
    JSONArray jsonAray = new JSONArray(tables);
    String JSS = jsonAray.toString();
    return JSS;
  }
  
  public String getTableColumns(String TableNames)
    throws JSONException, SQLException
  {
    Map tablesAndCol = this.newCom.getTableColumns(TableNames);
    
    JSONObject tableMap = new JSONObject(tablesAndCol);
    String Stringmap = tableMap.toString();
    return Stringmap;
  }
  
  public boolean checkVersion(String versionToCheck, String dbType)
    throws Exception
  {
    return this.newCom.checkVersion(versionToCheck, dbType);
  }
}
