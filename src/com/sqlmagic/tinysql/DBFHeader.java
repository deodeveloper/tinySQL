/*
 * DBFHeader.java
 * tinySQL, manipulation of the first 32 bytes of a dBase III header
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:31:13 $
 * $Revision: 1.1 $
 *
 */
package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;


/**
dBase III header read/write access (bytes 0 - 31) <br> 
The column definitions are not read
@author Brian Jepson <bjepson@home.com>
@author Marcel Ruff <ruff@swand.lake.de> Added write access to dBase and JDK 2 support
*/
public class DBFHeader
{
  String tableName = null;
  short file_type = 0;   // = 0x03 without .DBT, 0x83 with .DBT (memo file)
  short file_update_year = 0;
  short file_update_month = 0;
  short file_update_day = 0;
  int numFields = 0;     // number of column definitions
  int numRecords = 0;    // number of data records
  int headerLength = 0;  // in bytes
  int recordLength = 0;  // length in bytes of one data row, including the beginning delete flag-byte

  /* 
     The dBase III header consists of 32 byte bulks:
     0-32    primary header info
     32      bytes for each column info (n times)
     The number of columns is calculated from the headerLength
  */
  final static int BULK_SIZE = 32;             // 
  final static int FLAG_INDEX = 0;             // = 0x03 without .DBT, 0x83 with .DBT (memo file)
  final static int DATE_INDEX = 1;             // 1=YY 2=MM 3=DD (last update)
  final static int NUMBER_OF_REC_INDEX = 4;    // 4-7
  final static int LENGTH_OF_HEADER_INDEX = 8; // 8-9
  final static int LENGTH_OF_REC_INDEX = 10;   // 8-11
  final static int RESERVED_INDEX = 12;        // 12-31


  /**
   * Constructs a DBFHeader, read the data from file <br> 
   * You need to supply an open file handle to read from
   * @param ff open file handle for read access
   */
  DBFHeader(RandomAccessFile ff) throws tinySQLException
  {
    try {
      ff.seek(FLAG_INDEX);
      file_type         = Utils.fixByte(ff.readByte());

      // get the last update date
      file_update_year  = Utils.fixByte(ff.readByte());
      file_update_month = Utils.fixByte(ff.readByte());
      file_update_day   = Utils.fixByte(ff.readByte());

      // a byte array to hold little-endian long data
      //
      byte[] b = new byte[4];

      // read that baby in...
      //
      ff.readFully(b);

      // convert the byte array into a long (really a double)
      // 4-7 number of records
      numRecords      =  (int)Utils.vax_to_long(b);

      // a byte array to hold little-endian short data
      //
      b = new byte[2];

      // get the data position (where it starts in the file)
      // 8-9 Length of header
      ff.readFully(b);
      headerLength        = Utils.vax_to_short(b);

      // find out the length of the data portion
      // 10-11 Length of Record
      ff.readFully(b);
      recordLength   = Utils.vax_to_short(b);

      // calculate the number of fields
      //
      numFields = (int) (headerLength - 33)/32;

      // skip the next 20 bytes - looks like this is not needed...
      //ff.skipBytes(20);
      // 12-31 reserved

      Utils.log("HEADER=" + this.toString());

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
   * Constructs a DBFHeader, read the data from file <br> 
   * You need to supply an open file handle to read from
   * @param numFields number of Columns
   * @param recordLength sum of all column.size plus 1 byte (delete flag)
   */
  DBFHeader(int numFields, int recordLength) throws tinySQLException
  {
    this.numFields = numFields;
    this.recordLength = recordLength;
    Utils.log("DBFHeader", "numFields=" + numFields + " recordLength=" + recordLength);
  }


  /**
  Create new dBase file and write the first 32 bytes<br> 
  the file remains opened
  @return file handle with read/write access
  */
  public RandomAccessFile create(String dataDir, String tableName
                                 ) throws tinySQLException
  {
    this.tableName = tableName;

    try {
      // make the data directory, if it needs to be make
      //
      mkDataDirectory(dataDir);

      // perform an implicit drop table.
      //
      dropTable(dataDir, tableName);

      String fullPath = dataDir + File.separator + tableName + dbfFileTable.dbfExtension;
      RandomAccessFile ff = new RandomAccessFile(fullPath, "rw");

      write(ff);

      // ftbl.close();

      return ff;

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
  write the first 32 bytes to file
  */
  public void write(RandomAccessFile ff) throws tinySQLException
  {
    try {
      //-----------------------------
      // write out the primary header
      ff.seek(FLAG_INDEX);
      ff.writeByte((byte)0x03);

      setTimestamp(ff); // set current date YY MM DD (dBase is not Y2K save)

      setNumRecords(ff, 0);

      setHeaderLength(ff, numFields);

      setRecordLength(ff, recordLength);

      setReserved(ff);

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /*
   * Make the data directory unless it already exists
   */
  void mkDataDirectory(String dataDir) throws NullPointerException
  {
    File dd = new File( dataDir );

    if (!dd.exists()) {
      dd.mkdir();
    }
  }

  public void setTimestamp(RandomAccessFile ff) throws tinySQLException
  {
    try {
      java.util.Calendar cal = java.util.Calendar.getInstance();
      cal.setTime(new java.util.Date());
      int dd = cal.get(java.util.Calendar.DAY_OF_MONTH);
      int mm = cal.get(java.util.Calendar.MONTH) + 1;
      int yy = cal.get(java.util.Calendar.YEAR);
      yy = yy % 100;          // Y2K problem: only 2 digits
      ff.seek(DATE_INDEX);
      ff.write(yy);
      ff.write(mm);
      ff.write(dd);
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
  Update the header (index 4-7) with the new number of records
  @param New number of records
  */
  public void setNumRecords(RandomAccessFile ff, int numRecords) throws tinySQLException
  {
    this.numRecords = numRecords;
    writeNumRecords(ff, numRecords);
  }


  /**
  Update the header (index 4-7) with the new number of records <br> 
  This is the static variant (use it if you don't want to obtain
  a DBFHeader instance
  @param New number of records
  */
  public static void writeNumRecords(RandomAccessFile ff, int numRecords) throws tinySQLException
  {
    try {
      byte[] b = Utils.intToLittleEndian(numRecords);
      ff.seek(NUMBER_OF_REC_INDEX);
      ff.write(b);
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
  Update the header (index 8-9) with the new number of records
  @param numFields number of columns (used to calculate header length)
  */
  public void setHeaderLength(RandomAccessFile ff, int numFields) throws tinySQLException
  {
    this.numFields = numFields;
    try {
      int headerLength = (DBFHeader.BULK_SIZE+1) + numFields * DBFHeader.BULK_SIZE;
      ff.seek(DBFHeader.LENGTH_OF_HEADER_INDEX);
      ff.write(Utils.shortToLittleEndian((short)headerLength));
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
  Update the header (index 10-11) with the length of one record
  @param recordLength Length of one data record (row)
  */
  public void setRecordLength(RandomAccessFile ff, int recordLength) throws tinySQLException
  {
    this.recordLength = recordLength;
    try {
      ff.seek(DBFHeader.LENGTH_OF_REC_INDEX);
      ff.write(Utils.shortToLittleEndian((short)recordLength));
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  /**
  Update the header (index 10-11) with the length of one record
  @param recordLength Length of one data record (row)
  */
  public void setReserved(RandomAccessFile ff) throws tinySQLException
  {
    try {
      ff.seek(DBFHeader.RESERVED_INDEX);
      byte[] reserved = Utils.forceToSize(null,
             DBFHeader.BULK_SIZE - DBFHeader.RESERVED_INDEX,
             (byte)0);
      ff.write(reserved);  // padding with \0!
    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  }


  static void dropTable (String dataDir, String fname) throws tinySQLException {
    try {

      // delFile(fname);
      Utils.delFile(dataDir, fname + dbfFileTable.dbfExtension);

    } catch (Exception e) {
      throw new tinySQLException(e.getMessage());
    }
  } 


}

