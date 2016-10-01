/*
 * dbfFileDatabaseMetaData.java
 */

package com.sqlmagic.tinysql;

/**
 * Comprehensive information about the database as a whole.
 *
 * Many of the methods here return lists of information in
 * the form of ResultSet objects.
 * You can use the normal ResultSet methods such as getString and getInt
 * to retrieve the data from these ResultSets.  If a given form of
 * metadata is not available, these methods should throw an SQLException.
 *
 * Some of these methods take arguments that are String patterns.  These
 * arguments all have names such as fooPattern.  Within a pattern String, "%"
 * means match any substring of 0 or more characters, and "_" means match
 * any one character. Only metadata entries matching the search pattern
 * are returned. If a search pattern argument is set to a null ref,
 * that argument's criteria will be dropped from the search.
 *
 * An SQLException will be thrown if a driver does not support a meta
 * data method.  In the case of methods that return a ResultSet,
 * either a ResultSet (which may be empty) is returned or a
 * SQLException is thrown.
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:30:57 $
 * $Revision: 1.1 $
 *
 */
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.Vector;
import java.io.File;


/**
dBase read/write access <br>
@author Brian Jepson <bjepson@home.com>
@author Marcel Ruff <ruff@swand.lake.de> Added DatabaseMetaData with JDK 2 support
@author Thomas Morgner <mgs@sherito.org> Changed DatabaseMetaData to use java.sql.Types.
*/
public class dbfFileDatabaseMetaData extends tinySQLDatabaseMetaData {
  private final String emptyString = "";

    public dbfFileDatabaseMetaData(Connection connection) {
        super(connection);
    }

    public String getDatabaseProductName()
    {
        return "tinySQL";
    }
    public String getDatabaseProductVersion()
    {
        return tinySQLGlobals.VERSION;
    }
    String getDataDir()
    {
        String url = ((dbfFileConnection)getConnection()).url;
        if (url.length() <= 13)
            return null;

        String dataDir = url.substring(13);
        return dataDir;
    }

    /**
     * Gets a description of all the standard SQL types supported by
     * this database. They are ordered by DATA_TYPE and then by how
     * closely the data type maps to the corresponding JDBC SQL type.
     *
     * <P>Each type description has the following columns:
     *  <OL>
     *  <LI><B>TYPE_NAME</B> String => Type name
     *  <LI><B>DATA_TYPE</B> short => SQL data type from java.sql.Types
     *  <LI><B>PRECISION</B> int => maximum precision
     *  <LI><B>LITERAL_PREFIX</B> String => prefix used to quote a literal
     *      (may be null)
     *  <LI><B>LITERAL_SUFFIX</B> String => suffix used to quote a literal
            (may be null)
     *  <LI><B>CREATE_PARAMS</B> String => parameters used in creating
     *      the type (may be null)
     *  <LI><B>NULLABLE</B> short => can you use NULL for this type?
     *      <UL>
     *      <LI> typeNoNulls - does not allow NULL values
     *      <LI> typeNullable - allows NULL values
     *      <LI> typeNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>CASE_SENSITIVE</B> boolean=> is it case sensitive?
     *  <LI><B>SEARCHABLE</B> short => can you use "WHERE" based on this type:
     *      <UL>
     *      <LI> typePredNone - No support
     *      <LI> typePredChar - Only supported with WHERE .. LIKE
     *      <LI> typePredBasic - Supported except for WHERE .. LIKE
     *      <LI> typeSearchable - Supported for all WHERE ..
     *      </UL>
     *  <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
     *  <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
     *  <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
     *      auto-increment value?
     *  <LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
     *      (may be null)
     *  <LI><B>MINIMUM_SCALE</B> short => minimum scale supported
     *  <LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused
     *  <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
     *  </OL>
     *
     * @return ResultSet - each row is a SQL type description
     * @exception SQLException if a database access error occurs
     */
      public ResultSet getTypeInfo() throws SQLException {
        tsResultSet jrs = new tsResultSet();

        tsColumn jsc = new tsColumn("TYPE_NAME");
        jsc.type = Types.CHAR;
        jsc.size = 10;
        jrs.addColumn (jsc);

        jsc = new tsColumn("DATA_TYPE");
        jsc.type = Types.INTEGER;
        jsc.size = 6;
        jrs.addColumn (jsc);

        jsc = new tsColumn("PRECISION");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("LITERAL_PREFIX");
        jsc.type = Types.CHAR;
        jsc.size = 1;
        jrs.addColumn (jsc);

        jsc = new tsColumn("LITERAL_SUFFIX");
        jsc.type = Types.CHAR;
        jsc.size = 1;
        jrs.addColumn (jsc);

        jsc = new tsColumn("CREATE_PARAMS");
        jsc.type = Types.CHAR;
        jsc.size = 20;
        jrs.addColumn (jsc);

        jsc = new tsColumn("NULLABLE");
        jsc.type = Types.INTEGER;
        jsc.size = 6;
        jrs.addColumn (jsc);

        jsc = new tsColumn("CASE_SENSITIVE");
        jsc.type = Types.BIT;
        jsc.size = 1;
        jrs.addColumn (jsc);

        jsc = new tsColumn("SEARCHABLE");
        jsc.type = Types.INTEGER;
        jsc.size = 6;
        jrs.addColumn (jsc);

         /*
         *  <LI><B>UNSIGNED_ATTRIBUTE</B> boolean => is it unsigned?
         *  <LI><B>FIXED_PREC_SCALE</B> boolean => can it be a money value?
         *  <LI><B>AUTO_INCREMENT</B> boolean => can it be used for an
         *      auto-increment value?
         *  <LI><B>LOCAL_TYPE_NAME</B> String => localized version of type name
         *      (may be null)
         *  <LI><B>MINIMUM_SCALE</B> short => minimum scale supported
         *  <LI><B>MAXIMUM_SCALE</B> short => maximum scale supported
         *  <LI><B>NUM_PREC_RADIX</B> int => usually 2 or 10
         */


        // NOTE: the Hashtable in tsRow expects always a String as its value!
        //       so i use the toString() method here
        //       Perhaps in future the real type should be pushed into the Hashtable?

        tsRow record = new tsRow();
        record.put("TYPE_NAME", dbfFile.typeToLiteral(Types.CHAR));    // "CHAR", String
        record.put("DATA_TYPE", new Integer(Types.CHAR).toString());
        record.put("PRECISION", new Integer(254).toString());
        record.put("LITERAL_PREFIX", "\"");
        record.put("LITERAL_SUFFIX", "\"");
        record.put("CREATE_PARAMS", new Integer(0).toString());
        record.put("NULLABLE", new Integer(typeNullableUnknown).toString());
        record.put("CASE_SENSITIVE", "N");
        record.put("SEARCHABLE", new Integer(typePredBasic).toString());
        jrs.addRow (record) ;

        record = new tsRow();
        record.put("TYPE_NAME", dbfFile.typeToLiteral(Types.FLOAT));    // "FLOAT", double
        record.put("DATA_TYPE", new Integer(Types.FLOAT).toString());
        record.put("PRECISION", new Integer(19).toString());
        record.put("LITERAL_PREFIX", emptyString);
        record.put("LITERAL_SUFFIX", emptyString);
        record.put("CREATE_PARAMS", new Integer(0).toString());
        record.put("NULLABLE", new Integer(typeNullableUnknown).toString());
        record.put("CASE_SENSITIVE", "N");
        record.put("SEARCHABLE", new Integer(typePredBasic).toString());
        jrs.addRow (record) ;

        record = new tsRow();
        record.put("TYPE_NAME", dbfFile.typeToLiteral(Types.BIT));     // "CHAR", boolean "YyNnTtFf"
        record.put("DATA_TYPE", new Integer(Types.BIT).toString());
        record.put("PRECISION", new Integer(1).toString());
        record.put("LITERAL_PREFIX", "\"");
        record.put("LITERAL_SUFFIX", "\"");
        record.put("CREATE_PARAMS", new Integer(0).toString());
        record.put("NULLABLE", new Integer(typeNullableUnknown).toString());
        record.put("CASE_SENSITIVE", "N");
        record.put("SEARCHABLE", new Integer(typePredBasic).toString());
        jrs.addRow (record) ;

        record = new tsRow();
        record.put("TYPE_NAME", dbfFile.typeToLiteral(Types.INTEGER));     // "INT", unsigned long
        record.put("DATA_TYPE", new Integer(Types.INTEGER).toString());
        record.put("PRECISION", new Integer(19).toString());
        record.put("LITERAL_PREFIX", emptyString);
        record.put("LITERAL_SUFFIX", emptyString);
        record.put("CREATE_PARAMS", new Integer(0).toString());
        record.put("NULLABLE", new Integer(typeNullableUnknown).toString());
        record.put("CASE_SENSITIVE", "N");
        record.put("SEARCHABLE", new Integer(typePredBasic).toString());
        jrs.addRow (record) ;

        record = new tsRow();
        record.put("TYPE_NAME", dbfFile.typeToLiteral(Types.DATE));     // "DATE", date
        record.put("DATA_TYPE", new Integer(Types.DATE).toString());
        record.put("PRECISION", new Integer(8).toString());
        record.put("LITERAL_PREFIX", "\"");
        record.put("LITERAL_SUFFIX", "\"");
        record.put("CREATE_PARAMS", new Integer(0).toString());
        record.put("NULLABLE", new Integer(typeNullableUnknown).toString());
        record.put("CASE_SENSITIVE", "N");
        record.put("SEARCHABLE", new Integer(typePredBasic).toString());
        jrs.addRow (record) ;

        return new tinySQLResultSet(jrs, (tinySQLStatement)null);
      }
/*
 * Gets a description of tables available in a catalog.
 *
 * Only table descriptions matching the catalog, schema, table
 * name and type criteria are returned.  They are ordered by
 * TABLE_TYPE, TABLE_SCHEM and TABLE_NAME.
 *
 * Each table description has the following columns:
 * 
 * TABLE_CAT String => table catalog (may be null)
 * TABLE_SCHEM String => table schema (may be null)
 * TABLE_NAME String => table name
 * TABLE_TYPE String => table type.  Typical types are "TABLE",
 *      "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
 *      "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
 * REMARKS String => explanatory comment on the table
 *
 * Note: Some databases may not return information for
 * all tables.
 *
 * @param catalog a catalog name; "" retrieves those without a
 * catalog; null means drop catalog name from the selection criteria
 * THIS VALUE IS IGNORED
 * @param schemaPattern THIS VALUE IS IGNORED
 * @param tableNamePattern a table name pattern, ´null´ or "%" delivers all
 *                         token will be handled as substrings
 * @param types a list of table types to include; null returns all DBF types
 *              only "TABLE" is supported, others like "VIEW", "SYSTEM TABLE", "SEQUENCE"
 *              are ignored.
 * @return ResultSet - each row is a table description
 * @exception SQLException if a database access error occurs
 * @see #getSearchStringEscape
 *
 * @author Thomas Morgner <mgs@sherito.org> Fill all needed columns, or some query tools will crash :(
 */
   public ResultSet getTables(String catalog, String schemaPattern,
            String tableNamePattern, String types[])
   {
      String dataDir = getDataDir();
      String tableName;
      File tableFile;
      tsColumn jsc;
      int i,dotAt;
      if (dataDir == null) return null;
      if (types == null)
      {
        types = new String[1];
        types[0] = "TABLE";
      }
      tsResultSet jrs = new tsResultSet();
/*
 *    Create the header for the tables ResultSet
 */
      try
      {
         jsc = new tsColumn("TABLE_CAT");
         jsc.type = Types.CHAR;    // CHAR max 254 bytes
         jsc.size = 10;
         jrs.addColumn (jsc);
     
         jsc = new tsColumn("TABLE_SCHEM");
         jsc.type = Types.CHAR;    // CHAR max 254 bytes
         jsc.size = 10;
         jrs.addColumn (jsc);
         
         jsc = new tsColumn("TABLE_NAME");
         jsc.type = Types.CHAR;    // CHAR max 254 bytes
         jsc.size = 10;
         jrs.addColumn (jsc);

         jsc = new tsColumn("TABLE_TYPE");
         jsc.type = Types.CHAR;    // CHAR max 254 bytes
         jsc.size = 40;
         jsc.defaultVal = "TABLE";
         jrs.addColumn (jsc);
      
         jsc = new tsColumn("TABLE_REMARKS");
         jsc.type = Types.CHAR;    // CHAR max 254 bytes
         jsc.size = 254;
         jrs.addColumn (jsc);
/*
 *       Add the MetaData by examining all the DBF files in the current 
 *       directory.
 */
         for ( int itype=0; itype < types.length; itype++ )
         {
            String type = types[itype];
            if (type == null) continue;
            String extension = null;
            if (type.equalsIgnoreCase("TABLE"))
               extension = dbfFileTable.dbfExtension; // ".DBF";
            if (extension == null) continue;
            Vector vec = Utils.getAllFiles(dataDir, extension);
            for ( i = 0; i < vec.size(); i++)
            {
               tableFile = (File)vec.elementAt(i);
               tableName = tableFile.getName().toUpperCase();
               dotAt = tableName.indexOf(".");
               if ( dotAt > -1 ) tableName = tableName.substring(0,dotAt);
               if (tableNamePattern == null ) tableNamePattern = "%";
               if ( tableNamePattern.equals("%") |
                   tableName.equalsIgnoreCase(tableNamePattern) )
               {
                  if (tableName.length() > jsc.size)
                     jsc.size = tableName.length();
                  tsRow record = new tsRow();
                  record.put("TABLE_NAME", tableName.toUpperCase());
                  record.put("TABLE_TYPE", "TABLE");
                  jrs.addRow (record) ;
               }
            }
         }
      } catch (Exception ex ) {
         System.out.println("Unable to create MetaData");
         ex.printStackTrace(System.out);
      }

      // This Resultset is not created by an statement 
      return new tinySQLResultSet(jrs, (tinySQLStatement)null);
   }
    /**
     * Gets a description of table columns available in
         * the specified catalog.
     *
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be null)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be null)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>DATA_TYPE</B> short => SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int => column size.  For char or date
     *      types this is the maximum number of characters, for numeric or
     *      decimal types this is precision.
     *  <LI><B>BUFFER_LENGTH</B> is not used.
     *  <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits
     *  <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int => is NULL allowed?
     *      <UL>
     *      <LI> columnNoNulls - might not allow NULL values
     *      <LI> columnNullable - definitely allows NULL values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing column (may be null)
     *  <LI><B>COLUMN_DEF</B> String => default value (may be null)
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused
     *  <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int => index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String => "NO" means column definitely
     *      does not allow NULL values; "YES" means the column might
     *      allow NULL values.  An empty string means nobody knows.
     *  </OL>
     *
     * @param catalog a catalog name; "" retrieves those without a
     * catalog; null means drop catalog name from the selection criteria
     * @param schemaPattern a schema name pattern; "" retrieves those
     * without a schema
     * @param tableNamePattern a table name pattern
     * @param columnNamePattern a column name pattern
     * @return ResultSet - each row is a column description
     * @exception SQLException if a database access error occurs
     * @see #getSearchStringEscape
     */
    public ResultSet getColumns(String catalog, String schemaPattern,
                String tableNamePattern, String columnNamePattern)
    {
      int i;
      String columnNameKey;
      try {
        String dataDir = getDataDir();

        Utils.log("Entering getColumns(tableNamePattern='" + tableNamePattern + "')");

        if (dataDir == null) return null;

        ResultSet tableRs = getTables(catalog, schemaPattern, tableNamePattern, null);

        tsResultSet jrs = new tsResultSet();

        tsColumn jsc = new tsColumn("TABLE_CAT");
        jsc.type = Types.CHAR;
        jsc.size = 9;
        jrs.addColumn (jsc);

        jsc = new tsColumn("TABLE_SCHEM");
        jsc.type = Types.CHAR;
        jsc.size = 11;
        jrs.addColumn (jsc);

        jsc = new tsColumn("TABLE_NAME");
        jsc.type = Types.CHAR;
        jsc.size = 10;
        jrs.addColumn (jsc);

        jsc = new tsColumn("COLUMN_NAME");
        jsc.type = Types.CHAR;
        jsc.size = 11;
        jrs.addColumn (jsc);

        jsc = new tsColumn("DATA_TYPE");
        jsc.type = Types.INTEGER;
        jsc.size = 6;
        jrs.addColumn (jsc);

        jsc = new tsColumn("TYPE_NAME");
        jsc.type = Types.CHAR;
        jsc.size = 9;
        jrs.addColumn (jsc);

        jsc = new tsColumn("COLUMN_SIZE");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("BUFFER_LENGTH");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("DECIMAL_DIGITS");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("NUM_PREC_RADIX");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("NULLABLE");
        jsc.type = Types.INTEGER;
        jsc.size = 8;
        jrs.addColumn (jsc);

        jsc = new tsColumn("REMARKS");
        jsc.type = Types.CHAR;
        jsc.size = 128;
        jrs.addColumn (jsc);

        jsc = new tsColumn("COLUMN_DEF");
        jsc.type = Types.CHAR;
        jsc.size = 128;
        jrs.addColumn (jsc);

        jsc = new tsColumn("SQL_DATA_TYPE");
        jsc.type = Types.INTEGER;
        jsc.size = 128;
        jrs.addColumn (jsc);

//      Several parameters missing.

        jsc = new tsColumn("IS_NULLABLE");
        jsc.type = Types.CHAR;
        jsc.size = 3;
        jrs.addColumn (jsc);

        while (tableRs.next()) { // process each DBF file and extract column info ...

          String tableName = tableRs.getString("TABLE_NAME");

          dbfFileTable tbl;
          try {
            tbl = new dbfFileTable(dataDir, tableName);
          } catch (Exception e) {
            continue; // ignore buggy and empty (zero byte size) DBF files
          }

          Utils.log("Accessing column info for table " + tableName);

          java.util.Hashtable column_info = tbl.column_info;
          for ( i = 0; i < tbl.columnNameKeys.size(); i++ )
          {
            columnNameKey = (String)tbl.columnNameKeys.elementAt(i);
            tsColumn tsc = (tsColumn)column_info.get(columnNameKey);
            // process each column of the current table ...
            tsRow record = new tsRow();
            record.put("TABLE_CAT", "");
            record.put("TABLE_SCHEM", "");
            record.put("TABLE_NAME", tableName);
            record.put("COLUMN_NAME", tinySQLGlobals.getLongName(tsc.name));
            record.put("DATA_TYPE", new Integer(tsc.type).toString());
            record.put("TYPE_NAME", dbfFile.typeToLiteral(tsc.type).toString());
            record.put("COLUMN_SIZE", new Integer(tsc.size).toString());
            record.put("DECIMAL_DIGITS", new Integer(tsc.decimalPlaces).toString());
            int nullable = columnNoNulls;
            if (tsc.notNull == true) nullable = columnNullable;
            record.put("NULLABLE", new Integer(nullable).toString());
            record.put("REMARKS", "noRemarks");
            String defaultVal = tsc.defaultVal;
            if (defaultVal == null) defaultVal = "";
            record.put("COLUMN_DEF", defaultVal);
            String isNullable = "NO";
            if (tsc.notNull == true) isNullable = "YES";
            record.put("IS_NULLABLE", isNullable);
/*
 *          Suppress any sorting of the ResultSet.  Column Metadata should
 *          be presented in the order the columns exist in the dBase header.
 */

            jrs.addRow (record,false) ;
          }

          tbl.close();
          tbl = null;
        }

        return new tinySQLResultSet(jrs,(tinySQLStatement)null);
      }
      catch (Exception e) {
        return null;
      }
    }
}
