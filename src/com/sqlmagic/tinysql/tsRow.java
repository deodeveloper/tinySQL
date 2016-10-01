/*
 *
 * tsRow.java - Row object for tinySQL.
 * 
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 * $Author: davis $
 * $Date: 2004/12/18 21:25:20 $
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

/*
 *
 * tsRow - an extension to Hashtable to hold a given row.
 *
 */
class tsRow extends Hashtable implements Comparable 
{
   Vector orderByColumns = (Vector)null;
   boolean debug=false;
  /**
   *
   * Given a column name, returns its value as a string.
   *
   * @param column the name of the column
   *
   */
  public void setOrderBy(Vector inputColumns)
  {
     orderByColumns = inputColumns;
  }
  public String columnAsString (String column) {
    return (String) get(column);
  }

  /**
   *
   * Given a tsColumn object, returns its value as a String.
   *
   * @param column the tsColumn object
   *
   */
   public String columnAsString (tsColumn column)
   {
      String outputString,valueString,functionName,argList,nextArg;
      StringBuffer functionBuffer;
      FieldTokenizer ft1,ft2;
/*
 *    Next try to retrieve as a group function, which will not have a
 *    tablename prefix.
 */
      outputString = (String)get(column.name);
      if ( outputString != (String)null ) return outputString;
      ft1 = new FieldTokenizer(column.name,'(',false);
      if ( ft1.countFields() == 2 )
      {
/*
 *       The column is a function.  If it is a group function, the value
 *       will be stored in the record.  Otherwise, the function value
 *       will be determined here by retrieving and processing all the
 *       columns in the function arguments.
 */
         outputString = (String)get(column.name);
         if ( outputString != (String)null ) return outputString;
         functionName = ft1.getField(0);
         argList = ft1.getField(1);
         ft2 = new FieldTokenizer(argList,',',false);
         functionBuffer = new StringBuffer();
/*
 *       Function arguments must consist of table.column names or constants.
 */
         while ( ft2.hasMoreFields() )
         {
            nextArg = ft2.nextField();
            valueString = (String)get(nextArg);
            if ( debug) System.out.println("Function " + functionName
            + " argument " + nextArg + " has value " + valueString);
/*
 *          If the valueString is null then it is a constant rather than
 *          a database column.  Remove enclosing quotes.
 */
            if ( valueString == (String)null ) 
               valueString = UtilString.removeQuotes(nextArg);
            else 
               valueString = valueString.trim();
            if ( functionName.equals("CONCAT") )
               functionBuffer.append(valueString);
         }
         outputString = functionBuffer.toString();
      } else if ( column.tableName == (String)null )  {
/*
 *       This is a constant.  Return the table name which will be equal to
 *       the constant value.
 */
         outputString = UtilString.removeQuotes(column.name);
      } else {
/*
 *       Retrieve as a simple database column.
 */ 
         outputString = (String)get(column.tableName + "." + column.name);
         if ( Utils.isDateColumn(column.type) ) 
         {
/*
 *          Format dates as DD-MON-YY for output.  Note that the value
 *          stored in the database may already be in this format because
 *          of incorrect storage of date strings prior to version 2.3.
 */
            try
            {
               outputString = UtilString.toStandardDate(outputString);
            } catch ( Exception dateEx ) {
               System.out.println(dateEx.getMessage());
            }  
         }
      }
      return outputString;
   }
   public int compareTo(Object inputObj)
   {
      String tableColumnName,thisString,inputString;
      tsColumn columnObject;
      tsRow inputRow;
      int i,columnType;
      double thisValue,inputValue;
      if ( orderByColumns == (Vector)null ) return 0;
      inputRow = (tsRow)inputObj;
      for ( i = 0; i < orderByColumns.size(); i++ )
      {
         columnObject = (tsColumn)orderByColumns.elementAt(i);
         tableColumnName = columnObject.name;
         columnType = columnObject.type;
         thisString = (String)get(tableColumnName);
         inputString = (String)inputRow.get(tableColumnName);
         if ( Utils.isCharColumn(columnType) | 
              Utils.isDateColumn(columnType) )
         {
            if ( thisString == (String)null | inputString == (String)null )
               continue;
            if ( thisString.compareTo(inputString) != 0 )
               return thisString.compareTo(inputString);
         } else if ( Utils.isNumberColumn(columnType) ) {
            thisValue = UtilString.doubleValue(thisString);
            inputValue = UtilString.doubleValue(inputString);
            if ( thisValue > inputValue ) return 1;
            else if ( thisValue < inputValue ) return -1;
         } else {
            System.out.println("Cannot sort unknown type");
         }
      }
      return 0;
   }
   public String toString()
   {
      StringBuffer outputBuffer = new StringBuffer();
      String columnName,columnValue;
      Enumeration e;
      for ( e = this.keys(); e.hasMoreElements(); )
      {
         columnName = (String)e.nextElement();
         columnValue = (String)this.get(columnName);
         outputBuffer.append(columnName + "=" + columnValue + " ");
      }
      return outputBuffer.toString();
   }
}

