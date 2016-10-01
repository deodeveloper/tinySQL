/*
 * tinySQLParser
 * 
 * $Author: davis $
 * $Date: 2004/12/18 21:28:17 $
 * $Revision: 1.1 $
 *
 * This simple token based parser replaces the CUP generated parser
 * simplifying extensions and reducing the total amount of code in 
 * tinySQL considerably.
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
 * Revision History;
 *
 * Written by Davis Swan in April, 2004.
 */

package com.sqlmagic.tinysql;

import java.io.*;
import java.util.*;
import java.text.*;
import java.sql.Types;

public class tinySQLParser
{
   Vector columnList,tableList,actionList,valueList,contextList,
   columnAliasList,columns;
   Hashtable tables;
   tinySQL dbEngine;
   tinySQLWhere whereClause;
   String tableName,tableAlias,dataDir;
   String statementType=(String)null;
   String lastKeyWord=(String)null,orderType=(String)null;
   String oldColumnName=(String)null,newColumnName=(String)null;
   String[] colTypeNames = {"INT","FLOAT","CHAR","DATE"};
   int[] colTypes = {Types.INTEGER,Types.FLOAT,Types.CHAR,Types.DATE};
   boolean distinct=false,defaultOrderBy=true;
   public tinySQLParser(InputStream sqlInput,tinySQL inputEngine)
      throws tinySQLException
   {
      StreamTokenizer st;
      FieldTokenizer ft;
      Reader r;
      String nextToken,upperField,nextField,keyWord=(String)null;
      StringBuffer cmdBuffer,inputSQLBuffer;
      int lastIndex,keyIndex;
      r = new BufferedReader(new InputStreamReader(sqlInput));
      dbEngine = inputEngine;
      actionList = new Vector();
      columnList = new Vector();
      columns = new Vector();
      columnAliasList = new Vector();
      contextList = new Vector();
      valueList = new Vector();
      tableName = (String)null;
      whereClause = (tinySQLWhere)null;
/*
 *    The tableList is a list of table names, in the optimal order 
 *    in which they should be scanned for the SELECT phrase.
 *    The Hashtable tables contains table objects keyed by table
 *    alias and name.
 */
      tableList = new Vector();
      tables = new Hashtable();
      tables.put("TABLE_SELECT_ORDER",tableList);
      try
      {
         st = new StreamTokenizer(r);
         st.eolIsSignificant(false);
         st.wordChars('\'','}');
         st.wordChars('?','?');
         st.wordChars('"','.');
         st.ordinaryChars('0','9');
         st.wordChars('0','9');
         cmdBuffer = new StringBuffer();
         inputSQLBuffer = new StringBuffer();
         while ( st.nextToken() != StreamTokenizer.TT_EOF)
         {
            if ( st.ttype == StreamTokenizer.TT_WORD )
               nextToken = st.sval.trim();
            else 
               continue;
            if ( inputSQLBuffer.length() > 0 ) inputSQLBuffer.append(" ");
            inputSQLBuffer.append(nextToken);
         }
         ft = new FieldTokenizer(inputSQLBuffer.toString(),' ',false);
         while ( ft.hasMoreFields() )
         {
            nextField = ft.nextField();
            upperField = nextField.toUpperCase();
            if ( statementType == (String)null ) 
            {
               statementType = upperField;
               lastIndex = getKeywordIndex(statementType,statementType);
               if ( lastIndex != 0 ) throwException(9);
               keyWord = statementType;
            } else {
               keyIndex = getKeywordIndex(statementType,upperField);
               if ( keyIndex < 0 )
               {
                  if ( cmdBuffer.length() > 0 ) cmdBuffer.append(" ");
                  cmdBuffer.append(nextField);
               } else {
                  setPhrase(keyWord,cmdBuffer.toString());
                  cmdBuffer = new StringBuffer();
                  keyWord = upperField;
                  if ( tinySQLGlobals.PARSER_DEBUG )
                     System.out.println("Found keyword " + keyWord);
               }
            }  
         }
         if ( keyWord != (String)null ) setPhrase(keyWord,cmdBuffer.toString());
         addAction();
         if ( tinySQLGlobals.PARSER_DEBUG )
            System.out.println("SQL:"+inputSQLBuffer.toString());
      } catch ( Exception ex ) {
         if ( tinySQLGlobals.DEBUG ) ex.printStackTrace(System.out);
         throw new tinySQLException(ex.getMessage());
      }
   }
/*
 * This method sets up particular phrase elements for the SQL command.
 * Examples would be a list of selected columns and tables for a SELECT
 * statement, or a list of column definitions for a CREATE TABLE
 * statement.  These phrase elements will be added to the action list
 * once the entire statement has been parsed.
 */
   public void setPhrase(String inputKeyWord,String inputString)
      throws tinySQLException
   {
      String nextField,upperField,colTypeStr,colTypeSpec,
      fieldString,syntaxErr,tempString,columnName,columnAlias;
      StringBuffer colTypeBuffer,concatBuffer;
      FieldTokenizer ft1,ft2,ft3;
      tsColumn createColumn;
      int i,j,k,lenc,colType,countFields;
/*
 *    Handle compound keywords.
 */
      if ( inputString == (String)null ) 
      {
         lastKeyWord = inputKeyWord;
         return;
      } else if ( inputString.trim().length() == 0 ) {
         lastKeyWord = inputKeyWord;
         return;
      }
      if ( tinySQLGlobals.PARSER_DEBUG )
         System.out.println("setPhrase " + inputString);
      ft1 = new FieldTokenizer(inputString,',',false);
      while ( ft1.hasMoreFields() )
      {
         nextField = ft1.nextField().trim();
         if ( tinySQLGlobals.PARSER_DEBUG )
            System.out.println(inputKeyWord + " field is " + nextField);
         upperField = nextField.toUpperCase();
         if ( inputKeyWord.equals("SELECT") )
         {
/*
 *          Check for the keyword DISTINCT
 */
            if (nextField.toUpperCase().startsWith("DISTINCT") )
            {
               distinct = true;
               nextField = nextField.substring(9).trim();
            }
/*
 *          Check for and set column alias.
 */
            ft2 = new FieldTokenizer(nextField,' ',false);
            columnName = ft2.getField(0);
            columnAlias = (String)null;
/*
 *          A column alias can be preceded by the keyword AS which will
 *          be ignored by tinySQL.
 */
            if ( ft2.countFields() == 2 ) columnAlias = ft2.getField(1);
            else if ( ft2.countFields() == 3 ) columnAlias = ft2.getField(2);
/*
 *          Check for column concatenation using the | symbol
 */
            ft2 = new FieldTokenizer(columnName,'|',false);
            if ( ft2.countFields() > 1 ) 
            {
               concatBuffer = new StringBuffer("CONCAT(");
               while ( ft2.hasMoreFields() )
               {
                  if ( concatBuffer.length() > 7 ) 
                     concatBuffer.append(",");
                  concatBuffer.append(ft2.nextField());
               }
               columnName = concatBuffer.toString() + ")";
            }
            columnList.addElement(columnName);
            columnAliasList.addElement(columnAlias);
            contextList.addElement(inputKeyWord);
         } else if ( inputKeyWord.equals("TABLE") ) {
/*
 *          If the input keyword is TABLE, update the statement type to be a 
 *          compound type such as CREATE_TABLE, DROP_TABLE, or ALTER_TABLE.
 */
            if ( !statementType.equals("INSERT") )
               statementType = statementType + "_TABLE";
            if ( statementType.equals("CREATE_TABLE") ) 
            {
/*
 *             Parse out the column definition.
 */
               ft2 = new FieldTokenizer(nextField,'(',false);
               if ( ft2.countFields() != 2 ) throwException(1);
               tableName = ft2.getField(0);
               fieldString = ft2.getField(1);
               ft2 = new FieldTokenizer(fieldString,',',false);
               while ( ft2.hasMoreFields() )
               {
                  tempString = ft2.nextField();
                  createColumn = parseColumnDefn(tempString);
                  if ( createColumn != (tsColumn)null )
                     columnList.addElement(createColumn);
               }
            } else if ( statementType.equals("DROP_TABLE") ) {
/*
 *             Handle dropping of non-existent tables
 */
               tableName = upperField;
               try
               {
                  validateTable(upperField,true);
               } catch ( Exception dropEx ) {
                  throw new tinySQLException("Table " + tableName 
                  + " does not exist.");
               }
            } else {
               tableName = upperField;
               validateTable(upperField,true);
            } 
         } else if ( inputKeyWord.equals("BY") ) {
/*
 *          Set up Group by and Order by columns.
 */
            if ( lastKeyWord == (String)null ) 
            {
               throwException(6);
            } else {
               ft3 = new FieldTokenizer(upperField,' ',false);
               columnList.addElement(ft3.getField(0));
               if ( ft3.countFields() == 2 )
               {
/*
 *                ASC or DESC are the only allowable directives after GROUP BY
 */
                  if ( ft3.getField(1).startsWith("ASC") |
                       ft3.getField(1).startsWith("DESC") )
                     orderType = ft3.getField(1);
                  else
                     throwException(7);
               }
               if ( lastKeyWord.equals("ORDER") ) defaultOrderBy = false;
               contextList.addElement(lastKeyWord);
            }
         } else if ( inputKeyWord.equals("DROP") ) {
/*
 *          Parse list of columns to be dropped.
 */
            statementType = "ALTER_DROP";
            ft2 = new FieldTokenizer(upperField,' ',false);
            while ( ft2.hasMoreFields() )
            {
               columnList.addElement(UtilString.removeQuotes(ft2.nextField()));
            }
         } else if ( inputKeyWord.equals("RENAME") ) {
/*
 *          Parse old and new column name.
 */
            statementType = "ALTER_RENAME";
            ft2 = new FieldTokenizer(upperField,' ',false);
            oldColumnName = ft2.getField(0);
            newColumnName = ft2.getField(1);
            if ( newColumnName.equals("TO") & ft2.countFields() == 3 )
               newColumnName = ft2.getField(2);
            if ( newColumnName.length() > 11 ) 
               newColumnName = tinySQLGlobals.getShortName(newColumnName);
         } else if ( inputKeyWord.equals("ADD") ) {
/*
 *          Parse definition of columns to be added.
 */
            statementType = "ALTER_ADD";
            createColumn = parseColumnDefn(nextField);
            if ( createColumn != (tsColumn)null )
               columnList.addElement(createColumn);
         } else if ( inputKeyWord.equals("FROM") ) {
/*
 *          Check for valid table 
 */
            tableName = upperField;
            validateTable(tableName);
         } else if ( inputKeyWord.equals("INTO") ) {
            ft2 = new FieldTokenizer(nextField,'(',false);
            if ( ft2.countFields() != 2 ) throwException(3);
            tableName = ft2.getField(0).toUpperCase();
            validateTable(tableName);
            fieldString = ft2.getField(1).toUpperCase();
            ft2 = new FieldTokenizer(fieldString,',',false);
            while ( ft2.hasMoreFields() )
            {
               tempString = UtilString.removeQuotes(ft2.nextField());
               columnList.addElement(tempString);
               contextList.addElement(inputKeyWord);
            }
         } else if ( inputKeyWord.equals("VALUES") ) {
            ft2 = new FieldTokenizer(nextField,'(',false);
            fieldString = ft2.getField(0);
            ft2 = new FieldTokenizer(fieldString,',',false);
            while ( ft2.hasMoreFields() )
            {
               tempString = UtilString.removeQuotes(ft2.nextField());
               tempString = UtilString.replaceAll(tempString,"''","'");
               valueList.addElement(tempString);
            }
         } else if ( inputKeyWord.equals("UPDATE") ) {
            tableName = nextField.toUpperCase();
            validateTable(tableName);
         } else if ( inputKeyWord.equals("SET") ) {
/*
 *          Parse the update column name/value pairs
 */
            ft2 = new FieldTokenizer(nextField,'=',false);
            if ( ft2.countFields() != 2 ) throwException(4);
            columnList.addElement(ft2.getField(0));
            contextList.addElement(inputKeyWord);
            valueList.addElement(UtilString.removeQuotes(ft2.getField(1)));
         } else if ( inputKeyWord.equals("WHERE") ) {
            whereClause = new tinySQLWhere(nextField,tables);
         } else if ( !inputKeyWord.equals("TABLE") ) {
            throwException(10);
         }
      }
      lastKeyWord = inputKeyWord;
   }
   public void validateTable(String tableSpec) throws tinySQLException
   {
      validateTable(tableSpec,false);
   }
   public void validateTable(String tableSpec,boolean closeTable) 
      throws tinySQLException
   {
/*
 *    Create a table object for each table used in the SELECT statement
 *    and store these objects in the tables Hashtable.  Save the original
 *    list of table names to set the default selection order.
 *
 *    If closeTable is true the table object will be closed after it is
 *    validated (for DROP TABLE and ALTER TABLE commands).
 */
      String tableName,tableAlias,tableNameAndAlias,sortName;
      tinySQLTable addTable,sortTable;
      boolean tableAdded;
      FieldTokenizer ftTable;
      int i,addRowCount,sortRowCount;
      ftTable = new FieldTokenizer(tableSpec,' ',false);
      tableName = ftTable.getField(0);
      tableAlias = (ftTable.getField(1,tableName)).toUpperCase();
      tableNameAndAlias = tableName + "->" + tableAlias;
      addTable = dbEngine.getTable(tableNameAndAlias);
      addTable.GoTop();
      addRowCount = addTable.GetRowCount();
      if ( closeTable ) addTable.close();
      if ( tinySQLGlobals.PARSER_DEBUG )
         System.out.println("Add table " + tableNameAndAlias + " to tables");
      tables.put(tableNameAndAlias,addTable);
      tableAdded = false;
      for (i = 0; i < tableList.size(); i++)
      {
         sortName = (String)tableList.elementAt(i);
         sortTable = (tinySQLTable)tables.get(sortName);
         sortRowCount = sortTable.GetRowCount();
/*
 *       Sort the table selections from smallest to largest table to 
 *       enhance the query performance.
 */
         if ( addRowCount > sortRowCount ) continue;
         tableList.insertElementAt(tableNameAndAlias,i);
         tableAdded = true;
         break;
      }
      if ( !tableAdded ) tableList.addElement(tableNameAndAlias);
      if ( tinySQLGlobals.PARSER_DEBUG ) 
      {
         System.out.println("Table selection order");
         for ( i = 0; i < tableList.size(); i++ )
         {
            sortName = (String)tableList.elementAt(i);
            sortTable = (tinySQLTable)tables.get(sortName);
            sortRowCount = sortTable.GetRowCount();
            System.out.println(sortName + " " + sortRowCount);
         }
      }
   }
/*
 * Validate the column specifications by checking against the tables.
 */
   public void validateColumns() throws tinySQLException
   {
      String columnName,columnAlias,columnContext;
      tsColumn columnObject;
      boolean selectStar;
      tinySQLTable jtbl;
      int i,j;
/*
 *    Check for a column named *
 */
      selectStar = false;
      for (i = 0; i < columnList.size(); i++)
      {
         columnName = (String)columnList.elementAt(i);
         columnContext = (String)contextList.elementAt(i);
         if ( columnName.equals("*") )
         {
            if ( !columnContext.equals("SELECT") )
               throw new tinySQLException("* must be a SELECT column.");
            selectStar = true;
            break;
         }
      }
      if ( selectStar) 
      {
/*
 *       A column * has been found.  Delete the existing list of SELECT
 *       columns and replace by using an enumeration variable to cycle through 
 *       the columns in the tables Hashtable.
 */
         for ( i = 0; i < columnList.size(); i++ )
         {
            columnContext = (String)contextList.elementAt(i);
            if ( columnContext.equals("SELECT") )
            {
               columnList.removeElementAt(i);
               contextList.removeElementAt(i);
               columnAliasList.removeElementAt(i);
            }
         }
         for ( i = 0; i < tableList.size(); i++ )
         {
            jtbl = (tinySQLTable)tables.get((String)tableList.elementAt(i));
/*
 *          Expand to all columns.
 */
            for ( j = 0; j < jtbl.columnNameKeys.size(); j++ )
            {
               columnName = (String)jtbl.columnNameKeys.elementAt(j);
               columnList.addElement(jtbl.table + "->" + jtbl.tableAlias
                        + "." + columnName);
               columnAliasList.addElement(columnName);
               contextList.addElement("SELECT");
            }
         }
      }
/*
 *    Build a column object for each selected column.
 */
      if ( tables == (Hashtable)null )
         System.out.println("*****Column validation - no tables defined.");
      for (i = 0; i < columnList.size(); i++)
      {
         columnName = (String)columnList.elementAt(i);
         columnContext = (String)contextList.elementAt(i);
         columnAlias = (String)null;
         if ( i < columnAliasList.size() )
            columnAlias = (String)columnAliasList.elementAt(i);
         columnObject = new tsColumn(columnName,tables,columnContext);
         columnObject.alias = UtilString.removeQuotes(columnAlias);
         columns.addElement(columnObject);
      }
   }
/*
 * Parse out the column definition for a CREATE statement.
 */
   public tsColumn parseColumnDefn(String columnDefn) throws tinySQLException
   {
      tsColumn createColumn;
      int i;
      FieldTokenizer ft;
      String columnName,fieldString,tempString,colTypeStr,colTypeSpec;
      ft = new FieldTokenizer(columnDefn.toUpperCase(),' ',false);
/*
 *    A column definition must consist of a column name followed by a 
 *    column specification.
 */
      if ( ft.countFields() < 2 ) throwException(2);
      columnName = ft.getField(0);
/*
 *    Check for quotes around a column name that may contain blanks.
 */
      if ( columnName.charAt(0) == '"' &
           columnName.charAt(columnName.length() - 1) == '"' )
         columnName = columnName.substring(1,columnName.length() - 1);
      if ( columnName.length() > 11 )
      {
         columnName = tinySQLGlobals.getShortName(columnName);
      }
      createColumn = new tsColumn(columnName);
      colTypeStr = "";
      for ( i = 1; i < ft.countFields(); i++ )
         colTypeStr += ft.getField(1);
      ft = new FieldTokenizer(colTypeStr,'(',false);
      colTypeStr = ft.getField(0);
      createColumn.size = 10;
      createColumn.decimalPlaces = 0;
      if ( colTypeStr.equals("FLOAT") ) 
      {
         createColumn.size = 12;
         createColumn.decimalPlaces = 2;
      }
      colTypeSpec = ft.getField(1);
      if ( !colTypeSpec.equals("NULL") )
      {
/*
 *       Parse out the scale and precision if supplied.
 */
         ft = new FieldTokenizer(colTypeSpec,',',false);
         createColumn.size = ft.getInt(0,8);
         createColumn.decimalPlaces = ft.getInt(1,0);
      }
      createColumn.type = Integer.MIN_VALUE;
      for ( i = 0; i < colTypeNames.length; i++ )
         if ( colTypeStr.equals(colTypeNames[i]) )
             createColumn.type = colTypes[i]; 
      if ( createColumn.type == Integer.MIN_VALUE ) throwException(8);
      if ( tinySQLGlobals.PARSER_DEBUG )
         System.out.println("Column " + createColumn.name 
         + ", type is " + createColumn.type + ",size is " + createColumn.size 
         + ",precision is " + createColumn.decimalPlaces);
      return createColumn;
   }
/*
 * This method is used to identify SQL key words, and the order in which they
 * should appear in the SQL statement.
 */
   public int getKeywordIndex(String inputContext,String inputWord)
   {
      String[][] sqlSyntax = {{"SELECT","FROM","WHERE","GROUP","ORDER","BY"},
        {"INSERT","INTO","VALUES"},
        {"DROP","TABLE"},
        {"DELETE","FROM","WHERE"},
        {"CREATE","TABLE"},
        {"UPDATE","SET","WHERE"},
        {"ALTER","TABLE","DROP","MODIFY","ADD","RENAME"}};
      int i,j;
      for ( i = 0; i < sqlSyntax.length; i++ )
      {
         for ( j = 0; j < sqlSyntax[i].length; j++ )
         {
            if ( sqlSyntax[i][0].equals(inputContext) &
                 sqlSyntax[i][j].equals(inputWord) )
               return j;
         }
      }
      return Integer.MIN_VALUE;
   }
/*
 * Add an action Hashtable to the list of actions
 */
   public void addAction () throws tinySQLException, CloneNotSupportedException
   {
      int i,columnCount;
      tsColumn checkColumn,orderColumn;
      Hashtable newAction =  new Hashtable();
      newAction.put("TYPE", statementType);
      if ( statementType.equals("SELECT") )
      {
         newAction.put("TABLES",tables);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
/*
 *       Validate the column specifications and expand * if present
 */
         validateColumns();
/*
 *       If no ORDER BY clause was specified, default to the list of
 *       SELECT columns.
 */
         if ( defaultOrderBy ) 
         {
            columnCount = columns.size();
            for ( i = 0; i < columnCount; i++ ) 
            {
               orderColumn = (tsColumn)(columns.elementAt(i));
               if ( orderColumn.getContext("SELECT") )
               {
                  orderColumn.addContext("ORDER");
               }
            }
         }
         newAction.put("COLUMNS",columns);
         if ( orderType != (String)null ) newAction.put("ORDER_TYPE",orderType);
         if ( distinct ) newAction.put("DISTINCT","TRUE");
      } else if ( statementType.equals("DROP_TABLE") ) {
         newAction.put("TABLE",tableName);
      } else if ( statementType.equals("CREATE_TABLE") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMN_DEF",columnList);
      } else if ( statementType.equals("ALTER_RENAME") ) {
         newAction.put("TABLE",tableName);
         newAction.put("OLD_COLUMN",oldColumnName);
         newAction.put("NEW_COLUMN",newColumnName);
      } else if ( statementType.equals("ALTER_ADD") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMN_DEF",columnList);
      } else if ( statementType.equals("ALTER_DROP") ) {
         newAction.put("TABLE",tableName);
         newAction.put("COLUMNS",columnList);
      } else if ( statementType.equals("DELETE") ) {
         newAction.put("TABLE",tableName);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
      } else if ( statementType.equals("INSERT") |
                  statementType.equals("UPDATE") ) {
         newAction.put("TABLE",tableName);
         if ( columnList.size() != valueList.size() ) throwException(5);
         newAction.put("COLUMNS",columnList);
         newAction.put("VALUES",valueList);
         if ( whereClause != (tinySQLWhere)null )
            newAction.put("WHERE",whereClause);
      }
      actionList.addElement(newAction);
   }
   public void throwException(int exceptionNumber) throws tinySQLException
   {
      throwException(exceptionNumber,(String)null);
   }
   public void throwException(int exceptionNumber, String parameter)
      throws tinySQLException
   {
      String exMsg = (String)null;
      if ( exceptionNumber == 1 )
         exMsg = "CREATE TABLE must be followed by a table name and a list"
         + " of column specifications enclosed in brackets.";
      else if ( exceptionNumber == 2 )
         exMsg = "A column specification must consist of a column name"
         + " followed by a column type specification.";
      else if ( exceptionNumber == 3 )
         exMsg = "INTO should be followed by a table name and "
         + "a list of columns enclosed in backets.";
      else if ( exceptionNumber == 4 )
         exMsg = "SET must be followed by assignments in the form"
         + " <columnName>=<value>.";
      else if ( exceptionNumber == 5 )
         exMsg = "INSERT statement number of columns and values provided"
         + " do not match.";
      else if ( exceptionNumber == 6 )
         exMsg = "BY cannot be the first keyword.";
      else if ( exceptionNumber == 7 )
         exMsg = "ORDER BY can only be followed by the ASC or DESC directives";
      else if ( exceptionNumber == 8 )
         exMsg = "Supported column types are INT,CHAR,FLOAT,DATE";
      else if ( exceptionNumber == 9 )
         exMsg = "Expecting SELECT, INSERT, ALTER, etc. in " + statementType;
      else if ( exceptionNumber == 10 )
         exMsg = "Unrecognized keyword ";
      throw new tinySQLException(exMsg);
   }
   public Vector getActions()
   {
      return actionList;
   }
}
