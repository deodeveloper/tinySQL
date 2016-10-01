/*
 *
 * Connection class for the dbfFile/tinySQL
 * JDBC driver
 *
 * A lot of this code is based on or directly taken from
 * George Reese's (borg@imaginary.com) mSQL driver.
 *
 * So, it's probably safe to say:
 *
 * Portions of this code Copyright (c) 1996 George Reese
 *
 * The rest of it:
 *
 * Copyright 1996 John Wiley & Sons, Inc. 
 * See the COPYING file for redistribution details.
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:30:05 $
 * $Revision: 1.1 $
 */
package com.sqlmagic.tinysql;


import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;


/**
dBase read/write access <br> 
@author Brian Jepson <bjepson@home.com>
@author Marcel Ruff <ruff@swand.lake.de> Added write access to dBase and JDK 2 support
*/
public class dbfFileConnection extends tinySQLConnection {

  private dbfFileDatabaseMetaData myMetaData = null;

  /**
   *
   * Constructs a new JDBC Connection object.
   *
   * @exception SQLException in case of an error
   * @param user the user name - not currently used
   * @param u the url to the data source
   * @param d the Driver object
   *
   */
  public dbfFileConnection(String user, String u, Driver d) 
         throws SQLException {
    super(user, u, d);
  }

  /**
   *
   * Returns a new dbfFile object which is cast to a tinySQL
   * object.
   *
   */
  public tinySQL get_tinySQL() {

     // if there's a data directory, it will
     // be everything after the jdbc:dbfFile:
     //
     if (url.length() > 13) {
       String dataDir = url.substring(13);
       return (tinySQL) new dbfFile(dataDir);
     }

     // if there was no data directory specified in the
     // url, then just use the default constructor
     //
     return (tinySQL) new dbfFile();

  }

   /**
    * This method retrieves DatabaseMetaData
    * @see java.sql.Connection#getMetData
    * @exception SQLException
    * @return a DatabaseMetaData object (conforming to JDK 2)
    *
    */
   public DatabaseMetaData getMetaData() throws SQLException {
     if (myMetaData == null)
       myMetaData = new dbfFileDatabaseMetaData(this);
     return (DatabaseMetaData)myMetaData;
   }
}
