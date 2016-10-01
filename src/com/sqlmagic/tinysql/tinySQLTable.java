/*
 * tinySQLTable - abstract class for physical table access under tinySQL
 *
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:26:51 $
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
 *
 */

package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;

/**
 * @author Thomas Morgner <mgs@sherito.org> ColType returns int (an value from
 * java.sql.Types).
 */
public abstract class tinySQLTable {

  public String table; // the name of the table
  
  public String tableAlias;

  // Hashtable to contain info about columns in the table
  //
  public Hashtable column_info = null;
  public Vector columnNameKeys = null;

  /**
   *
   * Returns the current number of records in this table
   *
   */
  public abstract int GetRowCount() throws tinySQLException;
  /**
   *
   * Closes the table.
   *
   */
  public abstract void close() throws tinySQLException;
  /**
   *
   * Checks to see if the file is Open or closed.
   *
   */
  public abstract boolean isOpen() throws tinySQLException;

  /**
   *
   * Returns the size of a column.
   *
   * @param column name of the column.
   *
   */
  public abstract int ColSize(String column) throws tinySQLException;
  /**
   *
   * Returns the decimal places for a column.
   *
   * @param column name of the column.
   *
   */
  public abstract int ColDec(String column) throws tinySQLException;

  /**
  @return Length in bytes of one row
  */
  public abstract int getRecordLength();

  /**
   *
   * Returns the datatype of a column.
   *
   * @param column name of the column.
   *
   */
  public abstract int ColType(String column) throws tinySQLException;

  /**
   *
   * Updates the current row in the table.
   *
   * @param c Ordered Vector of column names
   * @param v Ordered Vector (must match order of c) of values
   *
   */
  public abstract void UpdateCurrentRow(Vector c, Vector v)
    throws tinySQLException;

  /**
   *
   * Position the record pointer at the top of the table.
   *
   */
  public abstract void GoTop() throws tinySQLException;

  /**
   *
   * Advance the record pointer to the next record.
   *
   */
  public abstract boolean NextRecord() throws tinySQLException;

  /**
   *
   * Insert a row. If c or v == null, insert a blank row
   *
   * @param c Ordered Vector of column names
   * @param v Ordered Vector (must match order of c) of values
   * Insert a blank row.
   *
   */
  public abstract void InsertRow(Vector c, Vector v) throws tinySQLException;

  /**
   *
   * Retrieve a column's string value from the current row.
   *
   * @param column the column name
   *
   */
  public abstract String GetCol ( String column ) throws tinySQLException ;

  /**
   *
   * Update a single column.
   *
   * @param column the column name
   * @param value the String value with which update the column
   *
   */
  public abstract void UpdateCol( String column, String value )
    throws tinySQLException;

  /**
   *
   * Delete the current row.
   *
   */
  public abstract void DeleteRow() throws tinySQLException;

  /** 
   *
   * Is the current row deleted?
   *
   */
  public abstract boolean isDeleted() throws tinySQLException;

}
