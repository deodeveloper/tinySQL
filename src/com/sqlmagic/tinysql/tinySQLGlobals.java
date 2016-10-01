/*
 * tinySQLGlobals
 * 
 * $Author: $
 * $Date:  $
 * $Revision:  $
 *
 * Static class to hold global values.
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
 * Written by Davis Swan in October, 2006.
 */

package com.sqlmagic.tinysql;

import java.io.*;
import java.util.*;
import java.text.*;

public class tinySQLGlobals
{
   static String dataDir = (String)null;
   static Vector longColumnNames;
   static String fileSep = System.getProperty("file.separator");
   static String newLine = System.getProperty("line.separator");
   static Hashtable DB_INDEX = new Hashtable();
   static String VERSION = "2.26h";
   static boolean DEBUG = false;
   static boolean PARSER_DEBUG = false;
   static boolean WHERE_DEBUG = false;
   static boolean EX_DEBUG = false;
   static int longNamesInFileCount;
   static boolean debug = false;
   public static void readLongNames(String inputDataDir)
   {
      String fullPath,longNameRecord;
      String[] fields;
      FieldTokenizer ft;
      File longColumnNameFile;
      dataDir = inputDataDir;
      BufferedReader longNameReader = (BufferedReader)null;
      fullPath = dataDir + fileSep + "TINYSQL_LONG_COLUMN_NAMES.dat";
      longColumnNames = new Vector();
      longColumnNameFile = new File(fullPath);
      if ( longColumnNameFile.exists() )
      {
         try
         {
            longNameReader = new BufferedReader(new FileReader(fullPath));
            while ( ( longNameRecord = longNameReader.readLine()) != null )
            {
               ft = new FieldTokenizer(longNameRecord,'|',false);
               fields = ft.getFields();
               longColumnNames.addElement(fields[0]);
               longColumnNames.addElement(fields[1]);
            }
            longNameReader.close();
            longNamesInFileCount = longColumnNames.size()/2;
            if ( debug )
               System.out.println("Long Names read: " + longNamesInFileCount);
         } catch ( Exception readEx ) {
            System.out.println("Reader exception " + readEx.getMessage());
            longNamesInFileCount = 0;
         }
      }
   }
/*
 * Method to add a long column name to the global Vector.  Note that
 * the entries are keyed by the short column name so that there is always
 * one and only one short name for any long name.
 */
   public static String addLongName(String inputColumnName)
   {
      String shortColumnName,countString;
      countString = "0000" + Integer.toString(longColumnNames.size()/2);
      shortColumnName = "COL" + countString.substring(countString.length() - 5);
      if ( debug )
         System.out.println("Add " + shortColumnName + "|" + inputColumnName);
      longColumnNames.addElement(shortColumnName);
      longColumnNames.addElement(inputColumnName);
      return shortColumnName;
   }  
/*
 * This method checks for the existence of a short column name for the
 * input name.  If one does not exist it is created.
 */ 
   public static String getShortName(String inputColumnName)
   {
      String shortColumnName=(String)null,longColumnName;
      int i;
      if ( inputColumnName.length() < 12 ) return inputColumnName;
      for ( i = 0; i < longColumnNames.size(); i+=2 )
      {
         longColumnName = (String)longColumnNames.elementAt(i+1);
         if ( longColumnName.equalsIgnoreCase(inputColumnName) )
         {
            shortColumnName = (String)longColumnNames.elementAt(i);
            if ( debug ) System.out.println("Return " + shortColumnName);
            return shortColumnName;
         }
      }
      if ( shortColumnName == (String)null )
      {
/*
 *       A short name has not been set up for this long name yet. 
 */
         if ( debug ) 
            System.out.println("Generate short name for " + inputColumnName);
         return addLongName(inputColumnName);
      }
      return inputColumnName;
   }
/*
 * Get the long column name for the input short name.  
 */
   public static String getLongName(String inputColumnName)
   {
      String longColumnName,shortColumnName;
      int i;
      for ( i = 0; i < longColumnNames.size(); i+=2 )
      {
         shortColumnName = (String)longColumnNames.elementAt(i);
         if ( shortColumnName.equalsIgnoreCase(inputColumnName) )
         {
            longColumnName = (String)longColumnNames.elementAt(i+1);
            if ( debug ) System.out.println("Return " + longColumnName);
            return longColumnName;
         }
      }
      return inputColumnName;
   }
   public static void writeLongNames()
   {
      FileWriter longNameWriter = (FileWriter)null;
      String fullPath,longColumnName,shortColumnName;
      int i;
      if ( longColumnNames.size() > longNamesInFileCount * 2 )
      {
/*
 *       The file needs to be updated.
 */
         fullPath = dataDir + fileSep + "TINYSQL_LONG_COLUMN_NAMES.dat";
         try
         {
            longNameWriter = new FileWriter(fullPath);
            if ( longNameWriter != (FileWriter)null )
            {
               for ( i = 0; i < longColumnNames.size(); i+=2 )
               {
                  shortColumnName = (String)longColumnNames.elementAt(i);
                  longColumnName = (String)longColumnNames.elementAt(i+1);
                  longNameWriter.write(shortColumnName + "|" + longColumnName
                  + newLine);
               }
               longNameWriter.close();
               longNamesInFileCount = longColumnNames.size()/2;
            } else {
               System.out.println("Unable to update long column names.");
            }
         } catch ( Exception writeEx ) {
            System.out.println("Write exception " + writeEx.getMessage());
         }
      }
   }
}
