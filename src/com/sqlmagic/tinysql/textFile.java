/*
 *
 * textFile - an extension of tinySQL for text file access
 *
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:25:06 $
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

import java.util.*;
import java.lang.*;
import java.io.*;

public class textFile extends tinySQL {

  // the data directory where textFile stores its files
  //
  static String dataDir = System.getProperty("user.home") + "/.tinySQL";

  /**
   * Creates a table given the name and a vector of
   * column definition (tsColumn) arrays.<br>
   *
   * @param table_name the name of the table
   * @param v a Vector containing arrays of column definitions.
   * @see tinySQL#CreateTable
   */
  void CreateTable ( String table_name, Vector v ) 
    throws IOException, tinySQLException {

    // make the data directory, if it needs to be make
    //
    mkDataDirectory();

    // perform an implicit drop table.
    //
    DropTable(table_name);

    // create the table definition file
    //
    FileOutputStream fdef = 
       new FileOutputStream( dataDir + "/" + table_name + ".def" );

    // open it as a DataOutputStream
    //
    DataOutputStream def = new DataOutputStream (fdef);

    // write out the column definition for the _DELETED column
    //
    def.writeBytes("CHAR|_DELETED|1\n");

    // write out the rest of the columns' definition. The
    // definition consists of datatype, column name, and
    // size delimited by a pipe symbol
    //
    for (int i = 0; i < v.size(); i++) {
       def.writeBytes( ((tsColumn) v.elementAt(i)).type + "|");
       def.writeBytes( ((tsColumn) v.elementAt(i)).name + "|");
       def.writeBytes( ((tsColumn) v.elementAt(i)).size + "\n");
    }

    // flush the DataOutputStream and jiggle the handle
    //
    def.flush();

    // close the file
    //
    fdef.close();
  }

  /**
   *
   * Return a tinySQLTable object, given a table name.
   *
   * @param table_name
   * @see tinySQL#getTable
   *
   */
  tinySQLTable getTable (String table_name) throws tinySQLException {
    return (tinySQLTable) new textFileTable (dataDir, table_name);
  }

  /**
   * Creates new Columns in table_name, given a vector of
   * column definition (tsColumn) arrays.<br> 
   *
   * ALTER TABLE table [ * ] ADD [ COLUMN ] column type 
   *
   * @param table_name the name of the table
   * @param v a Vector containing arrays of column definitions.
   * @see tinySQL#AlterTableAddCol
   */
  void AlterTableAddCol ( String table_name, Vector v ) 
    throws IOException, tinySQLException {

      throw new tinySQLException("ALTER TABLE ADD is not supported");
  }

  /**
   * Changes the name of a column
   *
   * ALTER TABLE table [ * ] RENAME [ COLUMN ] TO [NEW_COLUMN]
   *
   */
  void AlterTableRenameCol ( String table_name, String oldName, String newName) 
    throws tinySQLException {

      throw new tinySQLException("ALTER TABLE RENAME is not supported");
  }

  /**
   * Deletes Columns from table_name, given a vector of
   * column definition (tsColumn) arrays.<br> 
   *
   * ALTER TABLE table DROP [ COLUMN ] column { RESTRICT | CASCADE }
   *
   * @param table_name the name of the table
   * @param v a Vector containing arrays of column definitions.
   * @see tinySQL#CreateTable
   */
  void AlterTableDropCol ( String table_name, Vector v ) 
    throws IOException, tinySQLException {

      throw new tinySQLException("ALTER TABLE DROP is not supported");
  }


  /**
   *
   * Drop a named table by deleting it and its associated
   * .def file.
   *
   * @param fname table name
   * @see tinySQL#DropTable
   *
   */
  void DropTable (String fname) throws tinySQLException {

    try {

      Utils.delFile(dataDir, fname);
      Utils.delFile(dataDir, fname + ".def");

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }

  } 


  /*
   *
   * Make the data directory unless it already exists
   *
   */
  void mkDataDirectory() throws NullPointerException {

    File dd = new File( dataDir );

    if (!dd.exists()) {
      dd.mkdir();
    }

  }


  /*
   * regression test, does no longer work
   */
  public static void main(String argv[]) {
//    textFile foo = new textFile();
//    tsResultSet trs = null; 
//    try {
//      trs = foo.sqlexec("CREATE TABLE test (name CHAR(10))");
//      trs = foo.sqlexec("INSERT INTO test (name) VALUES('test')");
//      trs = foo.sqlexec("SELECT name FROM test");
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//
//    tsRow row = trs.rowAt(0);
//
//    tsColumn column = trs.columnAtIndex(0);
//    String colval     = row.columnAsString(column);
//
//    if (colval.startsWith("test")) {
//      System.out.println("textFile driver installed correctly.");
//    } else {
//      System.out.println("Test was not successful :-(");
//      System.out.println("Got \"" + colval + "\", expected \"test\"");
//    }
//
  }
}

