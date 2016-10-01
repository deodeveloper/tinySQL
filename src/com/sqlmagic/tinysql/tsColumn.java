/*
 * tsColumn.java - Column Object for tinySQL.
 * 
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:25:35 $
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
import java.text.*;
import java.sql.Types;

/*
 * Object to hold column metadata and value
 * Example for a column_def entity:
 * phone  CHAR(30)  DEFAULT '-'  NOT NULL
 *
 * @author Thomas Morgner <mgs@sherito.org> type is now integer 
 * and contains one of the java.sql.Types Values
 */
class tsColumn implements Cloneable
{
   public String name = null;      // the column's name
   public String alias = null;      // the column's definition
   public String longName = null;      // the column's long name ( > 11 chars)
   public Vector contextList;    // the columns context (SELECT,ORDER,etc.)
   public int type = -1;      // the column's type
                                   // dBase types:
                                   // 'C' Char (max 254 bytes)
                                   // 'N' '-.0123456789' (max 19 bytes)
                                   // 'L' 'YyNnTtFf?' (1 byte)
                                   // 'M' 10 digit .DBT block number
                                   // 'D' 8 digit YMD
   public int    size = 0;         // the column's size
   public int decimalPlaces = 0;   // decimal places in numeric column
   public String defaultVal = null;// not yet supported
   public int position = 0;        // internal use
   public String tableName = ""; // the table which "owns" the column
   public tinySQLTable columnTable = null;
   public String newLine = System.getProperty("line.separator");
   String functionName = (String)null;  // Function name
   String functionArgString = (String)null;  // Function arguments
   Vector functionArgs = (Vector)null; // Function arguments as columns
   boolean notNull = false;
   boolean valueSet = false;
   String stringValue = (String)null;
   int intValue = Integer.MIN_VALUE;
   float floatValue = Float.MIN_VALUE;
   SimpleDateFormat fmtyyyyMMdd = new SimpleDateFormat("yyyy-MM-dd");
   Calendar today = Calendar.getInstance();
   boolean isConstant = false;
   boolean groupedColumn = false;
/*
 * The constructor creates a column object using recursion if this is a 
 * function.
 */
   tsColumn (String s) throws tinySQLException
   {
      this(s,(Hashtable)null,"SELECT");
   }
   tsColumn (String s, Hashtable tableDefs,String inputContext)
      throws tinySQLException
   {
      FieldTokenizer ft,ftArgs;
      int i,j,numericType,nameLength,dotAt,argIndex;
      String upperName,checkName,nextArg;
      tinySQLTable jtbl;
      tsColumn tcol;
      Vector t;
      Enumeration col_keys;
      name = s;
      longName = name;
      nameLength = name.length();
      contextList = new Vector();
      contextList.addElement(inputContext);
      ft = new FieldTokenizer(name,'(',false);
      if ( ft.countFields() == 2 ) 
      {
/*
 *       This is a function rather than a simple column or constant
 */
         functionName = ft.getField(0).toUpperCase();
         if ( functionName.equals("COUNT") )
         {
            type = Types.INTEGER;
            size = 10;
            intValue = Integer.MIN_VALUE;
            groupedColumn = true;
         } else if ( functionName.equals("SUM") ) {
            type = Types.FLOAT;
            size = 10;
            groupedColumn = true;
         } else if ( functionName.equals("TO_DATE") ) {
            type = Types.DATE;
            size = 10;
         } else if ( functionName.equals("CONCAT") |
                     functionName.equals("UPPER") |
                     functionName.equals("SUBSTR") |
                     functionName.equals("TRIM") ) {
            type = Types.CHAR;
         }
         functionArgString = ft.getField(1);
         ftArgs = new FieldTokenizer(functionArgString,',',false);
         functionArgs = new Vector();
         argIndex = 0;
         while ( ftArgs.hasMoreFields() )
         {
            nextArg = ftArgs.nextField();
            tcol = new tsColumn(nextArg,tableDefs,inputContext);
            if ( tcol.isGroupedColumn() ) groupedColumn = true;
/*
 *          MAX and MIN functions can be either FLOAT or CHAR types
 *          depending upon the type of the argument.
 */
            if ( functionName.equals("MAX") | functionName.equals("MIN") ) 
            {
               if ( argIndex > 0 ) 
                  throw new tinySQLException("Function can only have 1 argument");
               groupedColumn = true;
               type = tcol.type;
               size = tcol.size;
            } else if ( functionName.equals("CONCAT") ) {
               type = Types.CHAR; 
               size += tcol.size;
            } else if ( functionName.equals("UPPER") ) {
               type = Types.CHAR; 
               size = tcol.size;
            } else if ( functionName.equals("TO_DATE") ) {
               type = Types.DATE; 
               size = 10;
            } else if ( functionName.equals("TRIM") ) {
               type = Types.CHAR; 
               size = tcol.size;
            } else if ( functionName.equals("SUBSTR") ) {
               type = Types.CHAR;
               if ( argIndex == 0 & tcol.type != Types.CHAR )
               {
                  throw new tinySQLException("SUBSTR first argument must be character");
               } else if ( argIndex == 1 ) {
                  if ( tcol.type != Types.INTEGER | tcol.intValue < 1 )
                  throw new tinySQLException("SUBSTR second argument "
                  + tcol.getString() + " must be integer > 0");
               } else if ( argIndex == 2 ) {
                  if ( tcol.type != Types.INTEGER | tcol.intValue < 1)
                  throw new tinySQLException("SUBSTR third argument "
                  + tcol.getString() + " must be integer > 0");
                  size = tcol.intValue;
               }  
            }
            argIndex++;
            functionArgs.addElement(tcol);
         }
      } else {
/*
 *       Check for SYSDATE
 */
         if ( name.toUpperCase().equals("SYSDATE") )
         {
            isConstant = true;
            type = Types.DATE;
            size = 10;
            notNull = true;
            valueSet = true;
            stringValue = fmtyyyyMMdd.format(today.getTime());
/*
 *          Check for a quoted string
 */
         } else if ( UtilString.isQuotedString(name) ) {
            isConstant = true;
            type = Types.CHAR;
            stringValue = UtilString.removeQuotes(name);
            if ( stringValue != (String)null )
            {
               size = stringValue.length(); 
               notNull = true;
               valueSet = true;
            }
         } else {
/*
 *          Check for a numeric constant
 */
            numericType = UtilString.getValueType(name);
            if ( numericType == Types.INTEGER )
            {
               intValue = Integer.valueOf(name).intValue();
               size = 10;
               type = numericType;
               isConstant = true;
               notNull = true;
               valueSet = true;
	    } else if ( numericType == Types.FLOAT ) {
               floatValue = Float.valueOf(name).floatValue();
               size = 10;
               type = numericType;
               isConstant = true;
               notNull = true;
               valueSet = true;
            } else {
/*
 *             This should be a column name. 
 */
               columnTable = (tinySQLTable)null;
               upperName = name.toUpperCase();
               if ( tinySQLGlobals.DEBUG )
                  System.out.println("Trying to find table for " + upperName);
               dotAt = upperName.indexOf(".");
               if ( dotAt > -1 ) 
               {
                  tableName = upperName.substring(0,dotAt);
                  if ( tableDefs != (Hashtable)null &
                       tableName.indexOf("->") < 0 )
                  {
                     t = (Vector)tableDefs.get("TABLE_SELECT_ORDER");
                     tableName = UtilString.findTableAlias(tableName,t);
                  }
                  upperName = upperName.substring(dotAt + 1);
/*
 *                Check to see if this column name has a short equivalent.
 */
                  if ( upperName.length() > 11 ) 
                  {
                     longName = name;
                     upperName = tinySQLGlobals.getShortName(upperName);
                  }
                  columnTable = (tinySQLTable)tableDefs.get(tableName);
               } else if ( tableDefs != (Hashtable)null ) {
/*
 *                Check to see if this column name has a short equivalent.
 */
                  if ( upperName.length() > 11 ) 
                  {
                     longName = name;
                     upperName = tinySQLGlobals.getShortName(upperName);
                  }
/*
 *                Use an enumeration to go through all of the tables to find
 *                this column.
 */
                  t = (Vector)tableDefs.get("TABLE_SELECT_ORDER");
                  for ( j = 0; j < t.size(); j++ )
                  {
                     tableName = (String)t.elementAt(j);
                     jtbl = (tinySQLTable)tableDefs.get(tableName);
                     col_keys = jtbl.column_info.keys();
/*
 *                   Check all columns.
 */
                     while (col_keys.hasMoreElements()) 
                     {
                        checkName = (String)col_keys.nextElement();
                        if ( checkName.equals(upperName) )
                        {
                           upperName = checkName;
                           columnTable = jtbl;
                           break;
                        }
                     }
                     if ( columnTable != (tinySQLTable)null ) break;
                  }
               } else {
                  if ( tinySQLGlobals.DEBUG )
                     System.out.println("No table definitions.");
               }
               if ( columnTable != (tinySQLTable)null ) 
               {
                  name = columnTable.table + "->" + columnTable.tableAlias
                     + "." + upperName;
                  type = columnTable.ColType(upperName);
                  size = columnTable.ColSize(upperName);
                  decimalPlaces = columnTable.ColDec(upperName);
                  tableName = columnTable.table + "->" + columnTable.tableAlias;
               }
            }
         }
      }
   }
/*
 * This function sets the column to a null value if the column belongs
 * to the input table, or the column is a function which has an
 * argument which belongs to the input table and whose value is null
 * if any argument is null.
 */
   public boolean clear()
   {
      return clear( (String)null );
   }
   public boolean clear(String inputTableName)
   {
      int i;
      tsColumn argColumn;
      boolean argClear;
      if ( functionName == (String)null )
      {
         if ( !isConstant )
         {
            if ( inputTableName == (String)null )
            {
               notNull = false;
               valueSet = false;
            } else if ( tableName == (String)null ) {
               notNull = false;
               valueSet = false;
            } else if ( tableName.equals(inputTableName) ) {
               notNull = false;
               valueSet = false;
            }
         }
      } else {
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            argColumn = (tsColumn)functionArgs.elementAt(i);
            argClear = argColumn.clear(inputTableName);
            if ( argClear & Utils.clearFunction(functionName) )
            {
               notNull = false;
               valueSet = false;
            }
         }
      }
      return isNull();
   }  
/*
 * This method updates the value of the column.  In the case of a function
 * only the argument values are updated, not the function as a whole. Functions
 * must be done using updateFunctions because of the requirement 
 * to evaluate summary functions only once per row.
 */
   public void update(String inputColumnName,String inputColumnValue)
      throws tinySQLException
   {
      int i;
      tsColumn argColumn;
      if ( isConstant | inputColumnName == (String)null ) return;
      if ( inputColumnName.trim().length() == 0 ) return;
      if ( functionName == (String)null )
      {
/*
 *       Only update the * column once per row.
 */
         if ( name.equals("*") & valueSet ) return;
         if ( inputColumnName.equals(name) | name.equals("*") ) 
         {
            if ( tinySQLGlobals.DEBUG )
            System.out.println("Set " + contextToString()
            + " column " + name + " = " + inputColumnValue.trim()); 
/*
 *          If this is a simple column value, reset to null before
 *          trying to interpret the inputColumnValue.
 */
            valueSet = true;
            notNull = false;
            stringValue = (String)null;
            intValue = Integer.MIN_VALUE;
            floatValue = Float.MIN_VALUE;
/*
 *          Empty string will be interpreted as nulls
 */
            if ( inputColumnValue == (String)null ) return;
            if ( inputColumnValue.trim().length() == 0 ) return;
            notNull = true;
            if ( type == Types.CHAR | type == Types.DATE | type == -1 )
            {
               stringValue = inputColumnValue;
            } else if ( type == Types.INTEGER & notNull ) {
               try
               {
                  intValue = Integer.parseInt(inputColumnValue.trim()); 
               } catch (Exception ex) {
                  throw new tinySQLException(inputColumnValue + " is not an integer.");
               }
            } else if ( type == Types.FLOAT & notNull ) {
               try
               {
                  floatValue = Float.valueOf(inputColumnValue.trim()).floatValue(); 
               } catch (Exception ex) {
                  throw new tinySQLException(inputColumnValue + " is not a Float.");
               }
            }
         }
      } else {
/*
 *       Update the function arguments.
 */
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            argColumn = (tsColumn)functionArgs.elementAt(i);
            argColumn.update(inputColumnName,inputColumnValue);
         }
      }
   }
/*
 * This method evaluates the value of functions.  This step must be kept
 * separate from the update of individual columns to prevent evaluation
 * of summary functions such as COUNT and SUM more than once, or when 
 * the row being processed will ultimately fail a where clause condition.
 */
   public void updateFunctions()
      throws tinySQLException
   {
      int i,startAt,charCount,day,monthAt,month,year;
      tsColumn argColumn;
      StringBuffer concatBuffer;
      FieldTokenizer ft;
      String[] ftFields;
      String months = "-JAN-FEB-MAR-APR-MAY-JUN-JUL-AUG-SEP-OCT-NOV-DEC-",
      monthName,dayField,monthField,yearField;
      if ( isConstant ) return;
      if ( functionName == (String)null ) return;
      if ( functionName.equals("CONCAT") )
      {
         concatBuffer = new StringBuffer();
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            argColumn = (tsColumn)functionArgs.elementAt(i);
            argColumn.updateFunctions();
            if ( argColumn.isValueSet() ) valueSet = true;
            if ( argColumn.notNull )
            {
               concatBuffer.append(argColumn.getString());
               notNull = true;
            }
         }
         stringValue = concatBuffer.toString();
      } else if ( functionName.equals("UPPER") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         if ( argColumn.notNull )
         {
            stringValue = argColumn.getString().toUpperCase();
            notNull = true;
         } 
      } else if ( functionName.equals("TRIM") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         if ( argColumn.notNull )
         {
            stringValue = argColumn.getString().trim();
            notNull = true;
         }
      } else if ( functionName.equals("SUBSTR") ) {
         if ( functionArgs.size() != 3 ) 
            throw new tinySQLException("Wrong number of arguments for SUBSTR");
         argColumn = (tsColumn)functionArgs.elementAt(1);
         startAt = argColumn.intValue;
         argColumn = (tsColumn)functionArgs.elementAt(2);
         charCount = argColumn.intValue;
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         if ( argColumn.notNull )
         {
            stringValue = argColumn.stringValue;
            if ( startAt < stringValue.length() - 1 & charCount > 0 )
            {
               stringValue = stringValue.substring(startAt - 1 ,startAt + charCount - 1);
               notNull = true;
            } else {
               stringValue = (String)null;
            }
         }
      } else if ( functionName.equals("COUNT") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
/*
 *       The COUNT function always returns a not null value
 */
         notNull = true;
         valueSet = true;
         if ( intValue == Integer.MIN_VALUE ) 
         {
            intValue = 0;
         } else {
            intValue = intValue + 1;
         }
      } else if ( functionName.equals("TO_DATE") ) {
/*
 *       Validate the TO_DATE argument
 */
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         type = Types.DATE;
         size = 10;
         if ( argColumn.notNull )
         {
            stringValue = argColumn.getString().trim();
            ft = new FieldTokenizer(stringValue,'-',false);
            ftFields = ft.getFields();
            if ( ftFields.length < 3 )
            { 
               throw new tinySQLException(stringValue + " is not a date with "
               + "format DD-MON-YY!");
            } else {
               try 
               {
                  day = Integer.parseInt(ftFields[0]);
                  if ( day < 1 | day > 31 )
                     throw new tinySQLException(stringValue + " day not " 
                     + "between 1 and 31.");
                  monthName = ftFields[1].toUpperCase();
                  monthAt = months.indexOf("-" + monthName + "-");
                  if ( monthAt == -1 )
                     throw new tinySQLException(stringValue + " month not " 
                     + "recognized.");
                  month = (monthAt + 4)/4;
                  year = Integer.parseInt(ftFields[2]);
                  if ( year < 0 | year > 2100 ) 
                     throw new tinySQLException(stringValue + " year not " 
                     + "recognized.");
/*
 *                Assume that years < 50 are in the 21st century, otherwise 
 *                the 20th.
 */
                  if ( year < 50 )
                  {
                     year = 2000 + year;
                  } else if ( year < 100 ) {
                     year = 1900 + year;
                  }
                  dayField = Integer.toString(day);
                  if ( dayField.length() < 2 ) dayField = "0" + dayField;
                  monthField = Integer.toString(month);
                  if ( monthField.length() < 2 ) monthField = "0" + monthField;
                  yearField = Integer.toString(year);
                  stringValue = yearField + "-" + monthField + "-" + dayField; 
               } catch (Exception dayEx ) {
                  throw new tinySQLException(stringValue + " exception " 
                  + dayEx.getMessage());
               }
            }
            notNull = true;
         }
      } else if ( functionName.equals("SUM") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         if ( argColumn.type == Types.CHAR | argColumn.type == Types.DATE )
            throw new tinySQLException(argColumn.name + " is not numeric!");
         if ( argColumn.notNull )
         {
            notNull = true;
            if ( floatValue == Float.MIN_VALUE ) 
            {
               floatValue = (float)0.0;
            } else {
               if ( argColumn.type == Types.INTEGER )
                  floatValue += new Integer(argColumn.intValue).floatValue();
               else
                  floatValue += argColumn.floatValue;
            }
         }
      } else if ( functionName.equals("MAX") | functionName.equals("MIN") ) {
         argColumn = (tsColumn)functionArgs.elementAt(0);
         argColumn.updateFunctions();
         if ( argColumn.isValueSet() ) valueSet = true;
         if ( argColumn.notNull )
         {
            notNull = true;
            if ( argColumn.type == Types.CHAR | argColumn.type == Types.DATE )
            {
               if ( stringValue == null )
               {
                  stringValue = argColumn.stringValue;
               } else {
/* 
 *                Update the max and min based upon string comparisions.
 */
                  if ( functionName.equals("MAX") &
                     ( argColumn.stringValue.compareTo(stringValue) > 0 ) )
                  {
                     stringValue = argColumn.stringValue;
                  } else if ( functionName.equals("MIN") &
                     ( argColumn.stringValue.compareTo(stringValue) < 0 ) ) {
                     stringValue = argColumn.stringValue;
                  }
               }
            } else if ( argColumn.type == Types.INTEGER ) {
/*
 *             Update max and min based upon numeric values.
 */
               if ( intValue == Integer.MIN_VALUE )
               {
                  intValue = argColumn.intValue;
               } else {
                  if ( functionName.equals("MIN") &
                     argColumn.intValue < intValue )
                     intValue = argColumn.intValue;
                  else if ( functionName.equals("MAX") &
                     argColumn.intValue > intValue )
                     intValue = argColumn.intValue;
               }
            } else if ( argColumn.type == Types.FLOAT ) {
               if ( floatValue == Float.MIN_VALUE ) 
               {
                  floatValue = argColumn.floatValue;
               } else {
                  if ( functionName.equals("MIN") &
                     argColumn.floatValue < floatValue )
                     floatValue = argColumn.floatValue;
                  else if ( functionName.equals("MAX") &
                     argColumn.floatValue > floatValue )
                     floatValue = argColumn.floatValue;
               }
            }
         }
      }
   }
   public boolean isGroupedColumn()
   {
      return groupedColumn;
   }
   public boolean isValueSet()
   {
      return valueSet;
   }
   public boolean isNotNull()
   {
      return notNull;
   }
   public boolean isNull()
   {
      return !notNull;
   }
/*
 * The following function compares this column to the input using
 * a "like" comparison using % as the wildcard.
 */
   public boolean like(tsColumn inputColumn) throws tinySQLException
   {
      FieldTokenizer ft;
      String nextField,firstField,lastField;
      boolean like;
      int foundAt;
      if ( !Utils.isCharColumn(type) | !Utils.isCharColumn(inputColumn.type) )
         throw new tinySQLException("Column " + name + " or " 
         + inputColumn.name + " is not character.");
      ft = new FieldTokenizer(inputColumn.stringValue,'%',true);
      like = true;
      foundAt = 0;
      firstField = (String)null;
      lastField = (String)null;
      while ( ft.hasMoreFields() )
      {
         nextField = ft.nextField();
         lastField = nextField;
/*
 *       If the first matching field is not the wildcare character
 *       then the test field must start with this string.
 */
         if ( firstField == (String)null ) 
         {
            firstField = nextField;
            if ( !firstField.equals("%") & !stringValue.startsWith(firstField) )
            {
               like = false;
               break;
            }
         }
         if ( !nextField.equals("%") )
         { 
            if ( stringValue.indexOf(nextField,foundAt) < 0 )
            {
               like = false;
               break;
            }
            foundAt = stringValue.indexOf(nextField,foundAt) + 1;
         }
      }
      if ( !lastField.equals("%") & !stringValue.endsWith(lastField) )
         like = false;
      if ( tinySQLGlobals.DEBUG )
         System.out.println("Is " + getString() + " like " +
      inputColumn.getString() +  " ? " + like); 
      return like;
   }
   public Object clone() throws CloneNotSupportedException
   {
      return super.clone();
   }
   public int compareTo(Object inputObj) throws tinySQLException
   {
      String thisString,inputString,thisYMD,inputYMD;
      tsColumn inputColumn;
      tsRow inputRow;
      int i,inputType,returnValue;
      double thisValue,inputValue;
      inputColumn = (tsColumn)inputObj;
      inputType = inputColumn.type;
      thisValue = Double.MIN_VALUE;
      inputValue = Double.MIN_VALUE;
      returnValue = 0;
      if ( Utils.isCharColumn(type) )
      {
/*
 *       Compare character types.
 */
         if ( !Utils.isCharColumn(inputType) )
         {
            throw new tinySQLException("Type mismatch between " 
            + getString() + " and " + inputColumn.getString());
         } else if ( stringValue == (String)null |
                   inputColumn.stringValue == (String)null ) {
            throw new tinySQLException("One of the values is NULL");
         } else {
            returnValue =  stringValue.compareTo(inputColumn.stringValue);
         }
      } else if ( Utils.isDateColumn(type) ) {
/*
 *       Compare date types.
 */
         if ( !Utils.isDateColumn(inputType) )
         {
            throw new tinySQLException("Type mismatch between " 
            + getString() + " and " + inputColumn.getString());
         } else if ( stringValue == (String)null |
                   inputColumn.stringValue == (String)null ) {
            throw new tinySQLException("One of the values is NULL");
         } else {
            inputYMD = UtilString.toStandardDate(inputColumn.stringValue);
            thisYMD = UtilString.toStandardDate(stringValue);
            returnValue =  thisYMD.compareTo(inputYMD);
         }
      } else if ( Utils.isNumberColumn(type) ) {
         if ( type == Types.INTEGER ) thisValue = (double)intValue;
         else if ( type == Types.FLOAT ) thisValue = (double)floatValue;
         if ( inputType == Types.INTEGER )
            inputValue = (double)inputColumn.intValue;
         else if ( inputType == Types.FLOAT )
            inputValue = (double)inputColumn.floatValue;
         if ( thisValue > inputValue ) returnValue = 1;
         else if ( thisValue < inputValue ) returnValue = -1;
      } else {
         System.out.println("Cannot sort unknown type");
      }
      if ( tinySQLGlobals.DEBUG )
         System.out.println("Comparing " + getString() + " to " +
         inputColumn.getString() +  " gave " + returnValue); 
      return returnValue;
   }
   public void addContext(String inputContext)
   {
      if ( inputContext != (String)null )
      {
         contextList.addElement(inputContext);
      }
   }
/*
 * This method checks to see if the column has the specified context.
 */
   public String contextToString()
   {
      StringBuffer outputBuffer = new StringBuffer();
      int i;
      for ( i = 0; i < contextList.size(); i++ )
      {
         if ( i > 0 ) outputBuffer.append(",");
         outputBuffer.append((String)contextList.elementAt(i));
      }
      return outputBuffer.toString();
   }
/*
 * This method returns the list of contexts as a string
 */
   public boolean getContext(String inputContext)
   {
      String nextContext;
      int i;
      for ( i = 0; i < contextList.size(); i++ )
      {
         nextContext = (String)contextList.elementAt(i);
         if ( nextContext == (String)null ) continue;
         if ( nextContext.equals(inputContext) ) return true;
      }
      return false;
   }
/*
 * This method returns the value of the column as a string
 */
   public String getString()
   {
      if ( !notNull ) return "null"; 
      if ( type == Types.CHAR | type == Types.DATE | type == -1 ) {
         return stringValue;
      } else if ( type == Types.INTEGER ) {
         if ( intValue == Integer.MIN_VALUE ) return (String)null;
         return Integer.toString(intValue);
      } else if ( type == Types.FLOAT ) {
         if ( floatValue == Float.MIN_VALUE ) return (String)null;
         return Float.toString(floatValue);
      }
      return (String)null;
   }
   public String toString()
   {
      int i;
      StringBuffer outputBuffer = new StringBuffer();
      if ( functionName == (String)null )
      {
         outputBuffer.append("-----------------------------------" + newLine
         + "Column Name: " + name + newLine 
         + "Table: " + tableName + newLine
         + "IsNotNull: " + notNull + newLine
         + "valueSet: " + valueSet + newLine
         + "IsConstant: " + isConstant + newLine
         + "Type: " + type + newLine
         + "Size: " + size + newLine
         + "Context: " + contextToString() + newLine
         + "Value: " + getString());
      } else {
         outputBuffer.append("Function: " + functionName + newLine
         + "IsNotNull: " + notNull + newLine
         + "Type: " + type + newLine
         + "Size: " + size + newLine
         + "Value: " + getString());
         for ( i = 0; i < functionArgs.size(); i++ )
         {
            outputBuffer.append(newLine + "Argument " + i + " follows" + newLine
            + ((tsColumn)functionArgs.elementAt(i)).toString() + newLine);
         }
      }
      return outputBuffer.toString();
   }
}
