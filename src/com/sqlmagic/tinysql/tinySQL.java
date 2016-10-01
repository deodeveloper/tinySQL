/*
 * tinySQL.java 
 *
 * A trivial implementation of SQL in an abstract class.
 * Plug it in to your favorite non-SQL data source, and
 * QUERY AWAY!
 *
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:23:50 $
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
 * Revision History:
 *
 * Major rewrite in 2003/4 by Davis Swan SQL*Magic Ltd.
 */

package com.sqlmagic.tinysql;

import java.util.*;
import java.lang.*;
import java.io.*;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author Thomas Morger <mgs@sherito.org> Changed tinySQL to reflect changes
 * in tinySQLResultSet - an instance of the connection is passed to a new 
 * resultset at construction time.
 *
 * When fetching a resultset, the number of rows to fetch can be set in the
 * statement. The builder will return when the given number of rows has been 
 * reached and will be restarted by tsResultSet when more rows are needed.
 */
public abstract class tinySQL {

  boolean debug=false,groupBreak=false,keepRecord=true;
  boolean exDebug=false,performDebug=false;
  Hashtable groupFunctions;
  String newLine = System.getProperty("line.separator");
  static tinySQLTable insertTable=(tinySQLTable)null;
  tinySQLWhere wc;
  // This is the InputStream from which the parser reads.
  // 
  private InputStream SQLStream; 

  // This is the last SQL Statement processed by sqlexec(String s).
  // Note that sqlexec() does *not* support this.
  //
  private tinySQLStatement sqlstatement = null;
/*
 *
 * Constructs a new tinySQL object.
 *
 */
  public tinySQL() {

  }

/*
 * Reads SQL statements from System.in() and returns a
 * tsResultSet for the last statement executed. This is
 * really only good for testing.
 * 
 * @exception tinySQLException
 */
  public tsResultSet sqlexec() throws tinySQLException {

    SQLStream = (InputStream) System.in;
    System.err.println("Reading SQL Statements from STDIN...");
    System.err.println("CRASHING AFTER THIS POINT IS SURE...");
    System.err.println("Have no Statement, no connection and no clue how to continue ...");
    return sql(null);

  }

/*
 * Processes the SQL Statement s and returns
 * a tsResultSet.
 *
 * @param s SQL Statement to execute
 * @exception tinySQLException
 */
  public tsResultSet sqlexec(tinySQLStatement s) throws tinySQLException
  {
     return sql(s);
  }
  public tsResultSet sqlexec(tinySQLPreparedStatement s)
     throws tinySQLException
  {
     return sql(s);
  }

/*
 *
 * Read SQL Statements from the SQLStream, and
 * return a result set for the last SQL Statement
 * executed.
 *
 * @returns the ResultSet or null, if no result set was created
 * @exception tinySQLException
 *
 */
   protected tsResultSet sql(Object s) throws tinySQLException 
   {
/*
 *    Build the ResultSet
 */
      tsResultSet rs = null;
      tinySQLTable jtbl;
      tinySQLPreparedStatement pstmt=(tinySQLPreparedStatement)null;
      boolean useTinyParser=true,distinct=false;
      Vector actions,columns,columnDefs,values,columnContexts,columnAliases;
      String actionType,orderType,tableName,statementType,byteString;
      Hashtable h,selectTables;
      byte[] bStream;
      ByteArrayInputStream st;
      int i;
      String actionString;
      groupBreak=false;
      keepRecord=true;
      statementType = s.getClass().getName();
      try
      {
/*
 *       Instantiate a new parser object which reads from the SQLStream.  This
 *       should probably be changed to a String at some point.  Note that 
 *       parsing is only done once for a PreparedStatement.
 */
         actions = (Vector)null;
         if ( statementType.endsWith("tinySQLPreparedStatement") )
         {
            pstmt = (tinySQLPreparedStatement)s;
            pstmt.updateActions(actions);
            actions = pstmt.getActions();
            byteString = pstmt.getSQLString();
            bStream = (byte[])null;
            if ( pstmt.getSQLString() != (String)null )
               bStream = pstmt.getSQLString().getBytes();
         } else if ( statementType.endsWith("tinySQLStatement") ) {
            bStream = ((tinySQLStatement)s).getSQLString().getBytes();
         } else {
            throw new tinySQLException("Unknown statement type" 
            + statementType);
         }
         if ( actions == (Vector)null ) 
         {
            st = new ByteArrayInputStream(bStream);
            SQLStream = (InputStream) st;
            tinySQLParser tinyp = new tinySQLParser(SQLStream,this);
            tinySQLGlobals.writeLongNames();
            actions = tinyp.getActions();
            if ( statementType.endsWith("tinySQLPreparedStatement") )
               pstmt.updateActions(actions);
         }
/*
 *       The actions Vector consists of a list of Hashtables.  Each of these
 *       action Hashtables contains elements required for a particular SQL
 *       statement. The following elements are used for various actions;
 *
 *       Type          Name           Description          
 *
 *       String        tableName      The name of the affected table for 
 *                                    CREATE,INSERT,UPDATE,DELETE actions.
 *
 *       Hashtable     selectTables   Hashtable of tables in a SELECT action. 
 *
 *       Vector        columns        A list of column names used by the
 *                                    the action.
 *
 *       Vector        columnContexts A list of Strings indicating the context
 *                                    for the elements in the columns Vector.
 *                                    Values can be SELECT,ORDER,GROUP.
 *      
 *       Vector        columnDefs     A list of column objects used by the 
 *                                    CREATE TABLE and ALTER TABLE ADD actions.
 *            
 *       Vector        values         A list of String values used in INSERT
 *                                    and UPDATE actions.
 *
 *       String        oldColumnName  Old column name for the 
 *                                    ALTER TABLE RENAME action.
 *
 *       String        newColumnName  New column name for the
 *                                    ALTER TABLE RENAME action.
 *            
 *       String        orderType      Type or ORDER BY - ASC or DESC.
 *
 *       String        distinct       Distinct rows only - TRUE or NULL
 *            
 *       tinySQLWhere  whereClause    An object containing the where clause
 *                                    which can be updated and queried.
 */            
         for (i = 0; i < actions.size(); i++) 
         {
            h = (Hashtable)actions.elementAt(i);
            actionType = (String)h.get("TYPE");
            if ( tinySQLGlobals.DEBUG )
               System.out.println("Action: " + actionType);
/*
 *          Many actions have a table specification.  If this one, build
 *          a table object to update the where clause if there is one.
 */
            tableName = (String) h.get("TABLE");
            wc = (tinySQLWhere) h.get("WHERE");
            if ( tableName != (String)null &  !actionType.equals("DROP_TABLE") &
               !actionType.equals("CREATE_TABLE") & !actionType.equals("INSERT")
               & !actionType.startsWith("ALTER") )
            {
               jtbl = getTable(tableName);
/*
 *             For prepared statements, store any table objects that 
 *             are created so that they can be explicitly closed when
 *             the statement processing is complete.
 */
               if ( statementType.endsWith("tinySQLPreparedStatement") )
                  pstmt.addTable(jtbl);
            }
            actionString = UtilString.actionToString(h);
            if ( debug ) System.out.println("ACTION: " + actionString);
            if ( actionType.equals("UPDATE") ) 
            {
/*     
 *             SQL UPDATE
 */            
               columns    = (Vector) h.get("COLUMNS");
               values     = (Vector) h.get("VALUES");
               UpdateStatement (tableName, columns, values, wc);
            } else if ( actionType.equals("DELETE") ) {
/*       
 *             SQL DELETE
 */            
               DeleteStatement (tableName, wc);
            } else if ( actionType.equals("SELECT") ) {
/*         
 *             SQL SELECT
 */            
               selectTables = (Hashtable) h.get("TABLES");
               columns = (Vector) h.get("COLUMNS");
               orderType = (String)h.get("ORDER_TYPE");
               if ( (String)h.get("DISTINCT") != (String)null ) 
                  distinct = true;
               rs = SelectStatement(selectTables,columns,
                                    wc,orderType,distinct,s);
            } else if ( actionType.equals("INSERT") ) {
/*        
 *             SQL INSERT
 */
               columns = (Vector) h.get("COLUMNS");
               values = (Vector) h.get("VALUES");
               InsertStatement (statementType, tableName, columns, values);
            } else if ( actionType.equals("CREATE_TABLE") ) {
/*
 *             SQL CREATE TABLE
 *
 *             CREATE TABLE User(user_oid  NUMBER(8)    NOT NULL,
 *                               userType  VARCHAR(80)  DEFAULT '-' NOT NULL,
 *                               PRIMARY KEY (user_oid))
 *
 *             -> DEFAULT / NOT NULL / PRIMARY KEY is not supported
 *
 */ 
               columnDefs = (Vector) h.get("COLUMN_DEF");
               CreateTable (tableName, columnDefs);
            } else if ( actionType.equals("ALTER_ADD") ) {
/*        
 *             SQL ALTER TABLE ADD
 */              
               columnDefs = (Vector) h.get("COLUMN_DEF");
               AlterTableAddCol (tableName, columnDefs);
            } else if ( actionType.equals("ALTER_DROP") ) {
/*         
 *             SQL ALTER TABLE DROP
 */             
               columns = (Vector) h.get("COLUMNS");
               AlterTableDropCol (tableName, columns);
            } else if ( actionType.equals("ALTER_RENAME") ) {
/*          
 *             SQL ALTER TABLE RENAME
 */             
               String oldColumnName = (String) h.get("OLD_COLUMN");
               String newColumnName = (String) h.get("NEW_COLUMN");
               AlterTableRenameCol(tableName, oldColumnName, newColumnName);
            } else if ( actionType.equals("DROP_TABLE") ) {
/*           
 *             SQL DROP TABLE
 */          
               DropTable( tableName );
            } else {
               System.out.println("Unrecognized action " + actionType);
            }
         }
      } catch (Exception e) {
         if ( tinySQLGlobals.EX_DEBUG ) e.printStackTrace(System.out);
         throw new tinySQLException(e.getMessage());
      }
      return rs;
   }
/*
 * Execute an SQL Select Statement
 */
   protected tsResultSet SelectStatement (Hashtable t,Vector c,
      tinySQLWhere w,String ot,boolean distinct,Object stmt)
      throws tinySQLException 
   {
      Hashtable tables;
      Vector tableList;
      tsResultSet jrs;
      tsColumn columnObject;
      int i;
/*
 *    Instantiate a new, empty tsResultSet
 */
      jrs = new tsResultSet(w, this);
      groupFunctions = new Hashtable();
      try
      {
      	jrs.setFetchSize(((tinySQLStatement)stmt).getFetchSize());
        jrs.setType(((tinySQLStatement)stmt).getResultSetType());
      } catch (SQLException sqle) {
        Utils.log ("Caught SQLException while setting Fetchsize and ResultSetType");
        Utils.log ("   This event is (should be) impossible!"); 
      }
      tables = t;
      tableList = (Vector)tables.get("TABLE_SELECT_ORDER");
/*
 *    Add the column objects to the ResultSet.
 */
      for (i = 0; i < c.size(); i++)
      {
         columnObject = (tsColumn)c.elementAt(i);
/*
 *       The column object is now added to the ResultSet
 */
         jrs.addColumn (columnObject);
         if ( debug ) System.out.println("Adding " 
         + columnObject.contextToString()
         + " column " + newLine + columnObject.toString() + newLine);
      }
      jrs.setState (1,tables,ot,distinct);
      contSelectStatement (jrs);
      return jrs;
  }
/*
 * Support function for restartable queries. Continue to
 * read the query result. The current state is taken from
 * tsResultSet. Proceed until maxFetchSize has reached.
 */
   protected void contSelectStatement (tsResultSet jrs)
      throws tinySQLException
   {
/*
 *    The table scan here is an iterative tree expansion, similar to
 *    the algorithm shown in the outline example in Chapter 5.
 */
      String columnName,columnString,whereStatus,tableAndAlias;
      boolean addOK;
      int i,rowCount;
      int level = jrs.getLevel ();
      tinySQLTable jtbl;
      tsColumn updateColumn;
      Hashtable tables = jrs.getTableState ();
      tinySQLWhere wc = jrs.getWhereClause();
/*
 *    Create a hashtable to enumerate the tables to be scanned and initialize
 *    with the first table name.
 */
      Hashtable tbl_list = new Hashtable();
      Vector groupFunction;
      Vector t = (Vector)tables.get("TABLE_SELECT_ORDER");
      String current = (String) t.elementAt(0);
      String firstColumn = "*";
      tbl_list.put( current, new Integer(1) );
/*
 *    Create a row object; this is added to the ResultSet.
 */
      tsRow record = new tsRow();
      Vector resultSet = new Vector();
/*
 *    Keep retrieving rows until we run out of rows to process.
 */
      while ( level > 0 )
      {
         boolean levelFound = false;
/*
 *       Find an item within the tbl_list which has the same level as the
 *       one we're on.
 */
         Enumeration keys = tbl_list.keys();
         while (keys.hasMoreElements()) 
         {
/*
 *          Get the next element in the "to be processed"
 *          Hashtable, and find out its level, storing this
 *          value in currLevel.
 */
            String hashkey = (String) keys.nextElement();
            int currLevel  = ((Integer) tbl_list.get(hashkey)).intValue();
/*
 *          As soon as an element is found whose level is equal to the 
 *          one currently being processed, grab it's primary key (the hashkey),
 *          flag levelfound, and break!
 */
            if (currLevel == level) 
            {
               current = hashkey; levelFound = true; break;
            }
         }
         boolean eof = false;        // did we hit eof?
         boolean haveRecord = false; // did we get a record or not?
/*
 *       If a table was found at the current level, then we should
 *       try to get another row from it.
 */
         if (levelFound) 
         {
/*
 *          Get the current table
 */
            jtbl = (tinySQLTable) tables.get(current);
            tableAndAlias = jtbl.table + "->" + jtbl.tableAlias;
            if ( tinySQLGlobals.WHERE_DEBUG )
               System.out.println("Processing level " + level 
               + " table " + tableAndAlias + "\n");
/*
 *          The following code is the start of setting up simple indexes
 *          for tinySQL.  The concept involves creating a new tinySQLCondition
 *          class, instances of which will be created by tinySQLWhere.  At least
 *          initially only conditions that are equal to a constant will be
 *          considered.  One of these conditions will be stored inside the
 *          dbfFileTable object. Calls to NextRecord would then have to add
 *          logic to check to see if an index exists.  The presence of a
 *          complete index would be indicated by a counter that was equal
 *          to the number of rows in the table.  In that case a single get
 *          would deliver the record numbers required for the where condition.
 *          The NextRecord function would return these record numbers in
 *          sequence.  If the index did not exist then new values in the 
 *          index Hashtable would be created.  None of this code has been 
 *          developed or implemented, let alone tested.
 *         
 *
 *          if ( wc != (tinySQLWhere)null )
 *             jtbl.setIndexCondition(wc.getIndexCondition(tableAndAlias));
 */
            if ( performDebug ) System.out.println("Selecting records from "
            + jtbl.table);
/*
 *          Skip to the next undeleted record; at some point,
 *          this will run out of records, and found will be false.
 */
            boolean found = false;
            while (jtbl.NextRecord()) 
            {
/*
 *             Clear the column values for this table before starting 
 *             to process the next row.
 */
               for ( i = 0; i < jrs.numcols(); i++ ) 
               {
                  updateColumn = jrs.columnAtIndex(i,true);
                  updateColumn.clear(tableAndAlias);
               }
               if ( wc != (tinySQLWhere)null )
                  wc.clearValues(tableAndAlias);
               if (!jtbl.isDeleted())
               {
/*
 *                Evaluate the where clause for each column in the table.  If
 *                it is false, skip to the next row.  Otherwise, add the
 *                column value to the output record.
 */
                  Enumeration cols = jtbl.column_info.keys();
                  found = true;
                  whereStatus = "TRUE";
                  while (cols.hasMoreElements())
                  {
                     columnName = tableAndAlias + "."
                     + (String)cols.nextElement();
                     columnString = jtbl.GetCol(columnName);
                     if ( wc != (tinySQLWhere)null ) 
                     {
                        whereStatus = wc.evaluate(columnName,columnString);
                        if ( whereStatus.equals("FALSE") )
                        {
/*
 *                         This column value caused the where clause to 
 *                         be FALSE.  Go to the next row in the table.
 */
                           found = false;
                           break;
                        }
                     }
/*
 *                   Update the ResultSet tsColumn values 
 */
                     jrs.updateColumns(columnName,columnString);
                  }
/*
 *                If no where condition has evaluated to false then this
 *                record is a candidate for output.  Break to the next table
 *                in this case for further WHERE processing.
 */
                  if ( found ) break;
               }
            }
            if ( found )
            {
               if ( tinySQLGlobals.DEBUG )
                  System.out.println("Build candidate record.");
               for ( i = 0; i < jrs.numcols(); i++ ) 
               {
                  updateColumn = jrs.columnAtIndex(i,true);
/*
 *                Evaluate all functions before adding the
 *                column to the output record.
 */
                  updateColumn.updateFunctions();
                  columnString = updateColumn.getString();
                  if ( updateColumn.isNotNull() )
                  {
                     record.put(updateColumn.name,columnString);
                  } else {
                     record.remove(updateColumn.name);
                  }
               }
               if ( performDebug )
                  System.out.println("Record is " + record.toString());
/*
 *             Since we were just able to get a row, then
 *             we are not at the end of file
 */
               eof = false;
/*
 *             If the table we are processing is not the last in
 *             the list, then we should increment level and loop to the top.
 */          
               if (level < t.size()) 
               {
/*
 *                Increment level
 */
                  level++;
/*
 *                Add the next table in the list of tables to
 *                the tbl_list, the Hashtable of "to be processed" tables.
 */
                  String next_tbl = (String) t.elementAt( level - 1);
                  tbl_list.put( next_tbl, new Integer(level) );
               } else {
/*
 *                If the table that was just processed is the last in
 *                the list, then we have drilled down to the bottom;
 *                all columns have values, and we can add it to the
 *                result set. The next time through, the program
 *                will try to read another row at this level; if it's
 *                found, only columns for the table being read will
 *                be overwritten in the tsRow.
 *                 
 *                Columns for the other table will be left alone, and 
 *                another row will be added to the result set. Here
 *                is the essence of the Cartesian Product which is
 *                being built here.
 */                
                  haveRecord = true;
               }
            } else {
/*
 *             We didn't find any more records at this level.
 *             Reset the record pointer to the top of the table,
 *             and decrement level. We have hit end of file here.
 */
               if ( wc != (tinySQLWhere)null )
                  wc.clearValues(tableAndAlias);
               level--;
               eof = true; 
               jtbl.GoTop();
            }
         } else {
/*
 *          No tables were found at this level; back up a level
 *          and see if there's any up there.
 */
            level--;
         }
/*
 *       If we got a record, then add it to the result set.
 */
         if (haveRecord)
         {
/*
 *          If group functions are involved, add records only after a break,
 *          which is defined as a change in all of the group columns.  
 *          Otherwise, update the current record.
 */
            if ( jrs.isGrouped() )
            {
               if ( groupBreak )
               {
                  addOK = jrs.addRow((tsRow)record.clone());
                  if (addOK == false)
                  {
                     jrs.setLevel (level);
                     return; 
                  }
                  groupBreak = false;
               } else {
                  jrs.updateRow( (tsRow) record.clone());
               }
            } else {
/*
 *             No group functions are involved.  Just add the record.
 */
               addOK = jrs.addRow((tsRow)record.clone());
               if (addOK == false)
               {
                  jrs.setLevel (level);
                  return; 
               }
            }
            firstColumn = "*";
         }
      }
/*
 *    Close all the tables
 */
      for ( i = 0; i < t.size(); i++ )
      {
         jtbl = (tinySQLTable)tables.get((String)t.elementAt(i));
         jtbl.close();
      }
/*
 *    return a result set
 */
      jrs.setLevel (0);
   }
/*
 * Delete rows which match a where clause.
 */
   private void DeleteStatement (String tableName, tinySQLWhere wc) 
      throws tinySQLException 
   {
      tinySQLTable jtbl;
      Hashtable tables;
      String columnName,columnString,whereStatus;
      Enumeration cols;
/* 
 *    Create a table object and put it in the Hashtable.
 */
      jtbl = getTable(tableName);
      tables = new Hashtable();
      tables.put(tableName, jtbl);
/*
 *    Process each row in the table ignoring deleted rows.
 */
      jtbl.GoTop();
      while (jtbl.NextRecord()) 
      {
         if (!jtbl.isDeleted()) 
         {
            cols = jtbl.column_info.keys();
            whereStatus = "TRUE";
            while (cols.hasMoreElements()) 
            {
               columnName = jtbl.table + "->" + jtbl.tableAlias + "."
               + (String)cols.nextElement();
               columnString = jtbl.GetCol(columnName);
/*
 *             Check the status of the where clause for each column value.
 */
               if ( wc != (tinySQLWhere)null )
                  whereStatus = wc.evaluate(columnName,columnString);
               if ( whereStatus.equals("FALSE") ) break;
            }
            if ( whereStatus.equals("TRUE") ) jtbl.DeleteRow();
            if ( wc != (tinySQLWhere)null )
               wc.clearValues(jtbl.table + "->" + jtbl.tableAlias);
         }
      }
      jtbl.close();
   }
/*
 * Update rows which match a WHERE clause
 */
   private void UpdateStatement(String tableName, Vector c, Vector v, tinySQLWhere wc)
       throws tinySQLException 
   {
/*
 *    Create a table object and put it in the Hashtable.
 */
      tinySQLTable jtbl = getTable(tableName);
      Hashtable tables = new Hashtable();
      tables.put(tableName, jtbl);
      String columnName,columnString,whereStatus;
/*
 *    Process each row in the table ignoring deleted rows.
 */
      jtbl.GoTop();
      while (jtbl.NextRecord()) 
      {
         if (!jtbl.isDeleted())
         {
            Enumeration cols = jtbl.column_info.keys();
            whereStatus = "TRUE";
            while (cols.hasMoreElements()) 
            {
/*
 *             Use the table name for the table alias for updates.
 */ 
               columnName = jtbl.table + "->" + jtbl.table 
               + "." + (String) cols.nextElement();
               columnString = jtbl.GetCol(columnName);
/*
 *             Check the status of the where clause for each column value.
 */
               if ( wc != (tinySQLWhere)null )
                  whereStatus = wc.evaluate(columnName,columnString);
               if ( whereStatus.equals("FALSE") ) break;
            }
            if ( whereStatus.equals("TRUE") ) jtbl.UpdateCurrentRow(c, v);
            if ( wc != (tinySQLWhere)null )
               wc.clearValues(jtbl.table + "->" + jtbl.tableAlias);
         }
      }
      jtbl.close();
   }
/*
 * Create the tinySQLTable object, then insert a row, and update
 * it with the c and v Vectors
 */
   private void InsertStatement (String statementType, String tableName,
                                 Vector c, Vector v) 
        throws tinySQLException 
   {
      String columnName,valueString;
      int i,columnType,columnSize;
      tinySQLTable jtbl=(tinySQLTable)null;
      double value;
      insertTable = getTable(tableName);
/*
 *    Check that the values supplied are the correct type and length.
 */
      for ( i = 0; i < c.size(); i++ )
      {
         columnName = (String)c.elementAt(i);
         valueString = (String)v.elementAt(i);
         if ( valueString == (String)null ) continue;
         valueString = UtilString.removeQuotes(valueString);
         valueString = UtilString.replaceAll(valueString,"''","'");
         columnType = insertTable.ColType(columnName);
         if ( Utils.isNumberColumn(columnType) ) 
         {
            try
            {
               value = Double.parseDouble(valueString);
            } catch (Exception e) {
               throw new tinySQLException("Insert failed: column "
               + columnName + " is numeric - found " + valueString);
            }
         } else if ( Utils.isDateColumn(columnType) ) {
            try
            {
/*
 *             Modify the input to be the standard YYYYMMDD format
 */
               if ( valueString.trim().length() > 0 )
                  v.setElementAt(UtilString.dateValue(valueString),i);
            } catch (Exception e) {
               throw new tinySQLException("Insert failed: " + e.getMessage());
            }
         }
         columnSize = insertTable.ColSize(columnName);
         if ( valueString.length() > columnSize )
         {
            throw new tinySQLException("Insert failed: string too long for "
            + " column " + columnName + " " 
            + Integer.toString(valueString.length())
            + " > " + Integer.toString(columnSize) + "<" + valueString +">");
         }
      }
      insertTable.InsertRow(c, v);
/*
 *    Close the table file that has just been updated unless this is a 
 *    PreparedStatement.  In that case an explicit close must be done
 *    on the statement object to close any open files.
 */
      if ( !statementType.endsWith("tinySQLPreparedStatement") )
         insertTable.close();
   }
/*
 * Creates a table given a tableName, and a Vector of column
 * definitions.
 *
 * The column definitions are an array of tsColumn
 */
  abstract void CreateTable (String tableName, Vector v) 
    throws IOException, tinySQLException;
/*
 * Creates new Columns given a tableName, and a Vector of
 * column definition (tsColumn) arrays.<br>
 *
 * ALTER TABLE table [ * ] ADD [ COLUMN ] column type 
 */
  abstract void AlterTableAddCol (String tableName, Vector v) 
    throws IOException, tinySQLException;

/*
 * Deletes Columns given a tableName, and a Vector of
 * column definition (tsColumn) arrays.<br>
 *
 * ALTER TABLE table DROP [ COLUMN ] column { RESTRICT | CASCADE }
 */
  abstract void AlterTableDropCol (String tableName, Vector v) 
    throws IOException, tinySQLException;
/*
 * Rename columns
 *
 * ALTER TABLE table RENAME war TO peace
 */
  abstract void AlterTableRenameCol (String tableName, String oldColumnName,
    String newColumnName) throws tinySQLException;
/*
 * Drops a table by name
 */
   abstract void DropTable (String tableName) throws tinySQLException;
/*
 * Create a tinySQLTable object by table name
 */
   abstract tinySQLTable getTable(String tableName) throws tinySQLException;

}
