/*
 * Extension of tinySQLTable which manipulates text files.
 *
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:26:34 $
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
import java.sql.Types;

/**
 * @author Thomas Morgner <mgs@sherito.org> Changed column types to java.sql.types.
 */
public class textFileTable extends tinySQLTable {

  // The data directory for tables
  //
  public String dataDir;

  // the object I'll use to manipulate the table
  //
  RandomAccessFile ftbl;

  // some constants that I don't actually use that much...
  //
  int COLUMN_SIZE = 0;
  int COLUMN_TYPE = 1;
  int COLUMN_POS  = 2;

  long record_number = 0; // current record
  long record_length;     // length of a record

  /**
   *
   * Constructs a textFileTable. This is only called by getTable()
   * in textFile.java.
   *
   * @param dDir data directory
   * @param table_name the name of the table
   *
   */
  textFileTable( String dDir, String table_name ) throws tinySQLException {

    dataDir = dDir;      // set the data directory
    table = table_name;  // set the table name

    // attempt to open the file in read/write mode
    //
    try {
      ftbl = new RandomAccessFile(dataDir + "/" + table_name, "rw");
    } catch (Exception e) {
      throw new tinySQLException("Could not open the file " + table + ".");
    }

    // read in the table definition
    //
    readColumnInfo();

  }
  public int GetRowCount() {
    // Not implemented get for text files.
    return 0;
  }
  /**
  @return Length in bytes of one row
  or 0 if not known
  */
  public int getRecordLength() {
    return 0;
  }


  /**
   *
   * close method. Try not to call this until you are sure 
   * the object is about to go out of scope.
   *
   */
  public void close() throws tinySQLException {

    try {
      ftbl.close();
    } catch (IOException e) {
      throw new tinySQLException(e.getMessage());
    }
  }
  /**
   *
   * Check if file is open for writing.
   *
   */
  public boolean isOpen() throws tinySQLException {

    return true;
  }

  /**
   *
   * Returns the size of a column
   *
   * @param column name of the column
   * @see tinySQLTable#ColSize
   *
   */
  public int ColSize(String column) {

    // retrieve the column info array from the column_info Hashtable
    //
    String info[] = (String[]) column_info.get(column);

    // return its size
    //
    return Integer.parseInt( info[COLUMN_SIZE] );

  }
  public int ColDec(String column) {
    // returns the decimal places for a column - not implemented
    // for text files.
    return 0;
  }


  /**
   *
   * Returns the datatype of a column.
   *
   * @param column name of the column.
   * @see tinySQLTable#ColType
   *
   * @author Thomas Morgner <mgs@sherito.org>
   * Q&D Hack, Just assume everybody uses java.sql.Types-IntegerConstants
   * as Type Declaration. Perhaps there could be an translation function,
   * which converts Strings to Integer-Types.
   */
  public int ColType(String column) {

    // retrieve the column info array from the column_info Hashtable
    //
    String info[] = (String[]) column_info.get(column);

    // return its datatype
    //
    return Integer.parseInt(info[COLUMN_TYPE]);

  }

  /**
   *
   * Updates the current row in the table.
   *
   * @param c Ordered Vector of column names
   * @param v Ordered Vector (must match order of c) of values
   * @see tinySQLTable#UpdateCurrentRow
   *
   */
  public void UpdateCurrentRow(Vector c, Vector v) throws tinySQLException {

    // the Vectors v and c are expected to have the 
    // same number of elements. It is also expected
    // that the elements correspond to each other,
    // such that value 1 of Vector v corresponds to
    // column 1 of Vector c, and so forth.
    //
    for (int i = 0; i < v.size(); i++) {

       // get the column name and the value, and
       // invoke UpdateCol() to update it.
       //
       String column = (String) c.elementAt(i);
       String value =  (String) v.elementAt(i);
       UpdateCol(column, value);
    }

  }

  /**
   *
   * Position the record pointer at the top of the table.
   *
   * @see tinySQLTable#GoTop
   *
   */
  public void GoTop() throws tinySQLException {

    try {
      ftbl.seek(0);
      record_number = 0;
    } catch (IOException e) {
      throw new tinySQLException(e.getMessage());
    }

  }

  /**
   *
   * Advance the record pointer to the next record.
   *
   * @see tinySQLTable#NextRecord
   *
   */
  public boolean NextRecord() throws tinySQLException {

    // if the record number is greater than zero, 
    // advance the pointer. Otherwise, we're on the first
    // record, and it hasn't been visited before.
    //
    if (record_number > 0) {

      // try to make it to the next record. An IOException
      // indicates that we have hit the end of file.
      //
      try {
        ftbl.seek( ftbl.getFilePointer() + record_length + 1);
      } catch (IOException e) {
        return false;
      }

    }

    // increment the record pointer
    //
    record_number++;

    // check for end of file, just in case...
    //
    try {
      if (ftbl.getFilePointer() == ftbl.length()) {
        return false;
      }
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }

    return true;

  }

  /**
   *
   * Insert a row. If c or v == null, insert a blank row
   *
   * @param c Ordered Vector of column names
   * @param v Ordered Vector (must match order of c) of values
   * @see tinySQLTable#InsertRow()
   *
   */
  public void InsertRow(Vector c, Vector v) throws tinySQLException {

    try {

      // go to the end of the file
      //
      ftbl.seek( ftbl.length() );

      // write out the deleted indicator
      //
      ftbl.write('N');

      // write out a blank record
      //
      for (int i = 1; i < record_length; i++) {
        ftbl.write(' ');
      }
      ftbl.write('\n');

      // reposition at start of current record
      //
      ftbl.seek( ftbl.getFilePointer() - (record_length + 1) );

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }

    if (c != null && v != null)
      UpdateCurrentRow(c, v);
  }

  /**
   *
   * Retrieve a column's string value from the current row.
   *
   * @param column the column name
   * @see tinySQLTable#GetCol
   *
   */
  public String GetCol(String column) throws tinySQLException {

    try {

      // get the column info
      //
      String info[] = (String[]) column_info.get(column);

      // retrieve datatype, size, and position within row
      //
      String datatype = info[COLUMN_TYPE];
      int size        = Integer.parseInt(info[COLUMN_SIZE]);
      int pos         = Integer.parseInt(info[COLUMN_POS]);

      // save the file pointer
      //
      long OldPosition = ftbl.getFilePointer();

      // read the whole line from this row.
      //
      String line = ftbl.readLine();

      // retrieve the column from the line we just read,
      // at offset pos, for length size
      //
      String result = line.substring(pos, pos + size);

      // restore the file pointer
      //
      ftbl.seek( OldPosition );

      // trim the result if it was numeric
      //
      if (datatype.equals("NUMERIC")) {
        return result.trim();
      } else {
        return result;  
      }

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }

  /**
   *
   * Update a single column.
   *
   * @param column the column name
   * @param value the String value with which update the column
   * @see tinySQLTable#UpdateCol
   *
   */
  public void UpdateCol( String column, String value ) throws tinySQLException {

    try {

      // read the column info
      //
      String info[] = (String[]) column_info.get(column);

      // retrieve datatype, size, and position within row
      //
      String datatype = info[COLUMN_TYPE];
      long size       = Long.parseLong(info[COLUMN_SIZE]);
      long pos        = Long.parseLong(info[COLUMN_POS]);

      // position the file pointer at the column
      // offset.
      //
      ftbl.seek( ftbl.getFilePointer() + pos );
      String writeval;

      if (value.length() > (int) size) {

        // truncate the value, if it exceeds the width 
        // of the column
        //
        writeval = value.substring(0, (int) size);

      }  else {

        // add some padding to the end of the string
        //
        StringBuffer pad = new StringBuffer();
        for (int p = 0; p < ((int) size) - value.length(); p++) {
          pad.append(" ");
        }
        writeval = value + pad.toString();
      }

      // write out the column
      //
      ftbl.writeBytes(writeval);

      // rewind the file pointer
      //
      ftbl.seek( ftbl.getFilePointer() - (pos + (long) writeval.length()) );

    } catch (Exception e) {
      e.printStackTrace();
      throw new tinySQLException(e.getMessage());
    }
  }

  /**
   *
   * Delete the current row.
   *
   * @see tinySQLTable#DeleteRow
   *
   */
  public void DeleteRow() throws tinySQLException {

    // this is real easy; just flip the value of the _DELETED column
    //
    UpdateCol("_DELETED", "Y");

  }

  /** 
   *
   * Is the current row deleted?
   *
   * @see tinySQLTable#isDeleted()
   *
   */
  public boolean isDeleted() throws tinySQLException {

    // this is real easy; just check the value of the _DELETED column
    //
    return (GetCol("_DELETED")).equals("Y");
  }

  // end methods implemented from tinySQLTable.java
  // the rest of this stuff is internal methods
  // for textFileTable
  //

  /*
   *
   * Reads in a table definition and populates the column_info
   * Hashtable
   *
   */
  void readColumnInfo() throws tinySQLException {

    try {

      column_info = new Hashtable();

      // Open an FileInputStream to the .def (table
      // definition) file
      //
      FileInputStream fdef = 
         new FileInputStream( dataDir + "/" + table + ".def" );

      // use a StreamTokenizer to break up the stream.
      //
      Reader r = new BufferedReader(
        new InputStreamReader(fdef));
      StreamTokenizer def = new StreamTokenizer (r);

      // set the | as a delimiter, and set everything between 
      // 0 and z as word characters. Let it know that eol is 
      // *not* significant, and that it should parse numbers.
      //
      def.whitespaceChars('|', '|');
      def.wordChars('0', 'z');
      def.eolIsSignificant(false);
      def.parseNumbers();

      // read each token from the tokenizer
      //
      while ( def.nextToken() != def.TT_EOF ) {

        // first token is the datatype 
        //
        // Q&D: Default is char value, numeric is special
        String datatype = String.valueOf (Types.CHAR);
        if (def.sval.equals ("NUMERIC"))
        {
          datatype = String.valueOf (Types.NUMERIC);
        }

        // get the next token; it's the column name
        //
        def.nextToken();
        String column   =  def.sval;

        // get the third token; it's the size of the column
        //
        def.nextToken();
        long size        =  (new Double(def.nval)).longValue();

        // create an info array
        //
        String[] info = new String[3];

        // store the datatype, the size, and the position
        // within the record (the record length *before* 
        // we increment it with the size of this column
        //
        info[COLUMN_TYPE] = datatype;
        info[COLUMN_SIZE] = Long.toString(size);
        info[COLUMN_POS]  = Long.toString(record_length);

        // this is the start position of the next column
        //
        record_length += size;  

        // store this info in the column_info hash,
        // keyed by column name.
        //
        column_info.put(column, info);

      }

      fdef.close(); // close the file

    } catch (Exception e) {

      throw new tinySQLException(e.getMessage());

    }

  }

}
