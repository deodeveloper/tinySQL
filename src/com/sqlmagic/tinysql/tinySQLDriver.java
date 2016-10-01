/*
 * tinySQLDriver - the tinySQLDriver abstract class
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
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:27:06 $
 * $Revision: 1.1 $
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307 USA
 */

package com.sqlmagic.tinysql;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.Driver;
import java.util.Properties;

public abstract class tinySQLDriver implements java.sql.Driver {

  /**
   *
   * Constructs a new tinySQLDriver
   *
   */
  public tinySQLDriver() {
  }

  /**
   *
   * Check the syntax of the URL.
   * @see java.sql.Driver#connect
   * @param url the URL for the database in question
   * @param info the properties object
   * @return null if the URL should be ignored, or a new Connection
   *         object if the URL is a valid tinySQL URL
   *
   */
  public Connection connect(String url, Properties info)
       throws SQLException {

    if( !acceptsURL(url) ) {
      return null;
    }
   
    // if it was a valid URL, return the new Connection
    //
    return getConnection(info.getProperty("user"), url, this);
  }

  /**
   *
   * Check to see if the URL is a tinySQL URL. It should start
   * with jdbc:tinySQL in order to qualify.
   *
   * @param url The URL of the database.
   * @return True if this driver can connect to the given URL.
   *
   */
  public boolean acceptsURL(String url) throws SQLException {

    // make sure the length is at least twelve
    // before bothering with the substring
    // comparison.
    //
    if( url.length() < 12 ) {
      return false;
    }

    // if everything after the jdbc: part is
    // tinySQL, then return true.
    //
    return url.substring(5,12).equals("tinySQL");

  }

  /**
   *
   * The getPropertyInfo method is intended to allow a generic GUI tool to
   * discover what properties it should prompt a human for in order to get
   * enough information to connect to a database.  Note that depending on
   * the values the human has supplied so far, additional values may become
   * necessary, so it may be necessary to iterate though several calls
   * to getPropertyInfo.
   *
   * @param url The URL of the database to connect to.
   * @param info A proposed list of tag/value pairs that will be sent on
   *          connect open.
   * @return An array of DriverPropertyInfo objects describing possible
   *          properties.  This array may be an empty array if no properties
   *          are required.
   *
   */
  public DriverPropertyInfo[] getPropertyInfo(String url,
					      java.util.Properties info)
       throws SQLException {
    return new DriverPropertyInfo[0];
  }
				
  /**
   *
   * Gets the driver's major version number.
   * @see java.sql.Driver#getMajorVersion
   * @return the major version
   *
   */
  public int getMajorVersion() {
    return 0;
  }

  /**
   *
   * Gets the driver's minor version
   * @see java.sql.Driver#getMinorVersion
   * @return the minor version
   *
   */
  public int getMinorVersion() {
    return 9;
  }

  /**
   *
   * Report whether the Driver is a genuine JDBC COMPLIANT (tm) driver.
   * Unfortunately, the tinySQL is "sub-compliant" :-(
   * 
   */
  public boolean jdbcCompliant() {
    return false;
  }

  /**
   *
   * Abstract method to return a tinySQLConnection object, typically
   * a subclass of the abstract class tinySQLConnection.
   *
   */
  public abstract tinySQLConnection getConnection
               (String user, String url, Driver d)
	       throws SQLException;

}
