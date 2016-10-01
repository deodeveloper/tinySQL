/*
 * PreparedStatement object for the tinySQL driver
 *
 * A lot of this code is based on or directly taken from
 * George Reese's (borg@imaginary.com) mSQL driver.
 *
 * So, it's probably safe to say:
 *
 * Portions of this code Copyright (c) 1996 George Reese
 *
 * The rest of it:
 *
 * Copyright 1996, Brian C. Jepson
 *                 (bjepson@ids.net)
 *
 * $Author: davis $
 * $Date: 2004/12/18 21:31:53 $
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


import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.ResultSet;
import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.Date;
import java.util.*;
import java.math.*;

/**
 * @author Thomas Morgner <mgs@sherito.org> statementString contains the last
 * used SQL-Query. Support for set/getFetchSize, ResultSets are created with a
 * reference to the creating statement
 */
public class tinySQLPreparedStatement implements java.sql.PreparedStatement {

  /**
   * Holds the original prepare stement including ? placeholders for 
   * values that will be replaced later.
   */
  private String statementString;
  /**
   * Holds the list of substitution values to be used in the prepared
   * statement.
   */
  private Vector substitute=(Vector)null;
  /**
   * Holds the list of table file objects so that they can be closed
   * when all the updates have been completed.
   */
  private Vector tableList=new Vector();
  /**
   * Holds the error message for invalid substitution index.
   */
  private String invalidIndex = (String)null;
  /**
   * Holds the last used queryString. execute() has to be synchronized,
   * to guarantee thread-safety
   */
  /**
   *
   * A connection object to execute queries and... stuff
   *
   */
  private tinySQLConnection connection;

  /**
   *
   * A result set returned from this query 
   *
   */
  private tinySQLResultSet result;
  /**
   *
   * A set of actions returned by tinySQLParser (see tinySQL.java)
   *
   */
  private Vector actions=(Vector)null;

  /**
   *
   * The max field size for tinySQL
   * This can be pretty big, before things start to break.
   *
   */
  private int max_field_size = 0;

  /**
   *
   * The max rows supported by tinySQL
   * I can't think of any limits, right now, but I'm sure some
   * will crop up...
   *
   */
  private int max_rows = 65536;

  /**
   *
   * The number of seconds the driver will allow for a SQL statement to
   * execute before giving up.  The default is to wait forever (0).
   *
   */
  private int timeout = 0;

  /**
   * How many rows to fetch in a single run. Default is now 4096 rows.
   */
  private int fetchsize = 4096;
  /**
   * Debug flag
   */
  private static boolean debug=false;
  /**
   *
   * Constructs a new tinySQLStatement object.
   * @param conn the tinySQLConnection object
   *
   */
  public tinySQLPreparedStatement(tinySQLConnection conn,String inputString) {

    int nextQuestionMark,startAt;
    connection = conn;
    startAt = 0;
    statementString = inputString;
    while ( (nextQuestionMark=statementString.indexOf("?",startAt)) > -1 )
    {
       if ( substitute == (Vector)null ) substitute = new Vector();
       substitute.addElement(new String(""));
       startAt = nextQuestionMark + 1;
    }
    invalidIndex = " is not in the range 1 to "
    + Integer.toString(substitute.size());
    if ( debug ) System.out.println("Prepare statement has " + substitute.size()
    + " parameters.");

  }

  /**
   *
   * Execute an SQL statement and return a result set.
   * @see java.sql.PreparedStatement#executeQuery
   * @exception SQLException raised for any errors
   * @param sql the SQL statement string
   * @return the result set from the query
   *
   */
  public synchronized ResultSet executeQuery()
       throws SQLException {

    // tinySQL only supports one result set at a time, so
    // don't let them get another one, just in case it's
    // hanging out.
    //
    result = null; 

    // create a new tinySQLResultSet with the tsResultSet
    // returned from connection.executetinySQL()
    //
    if ( debug) System.out.println("executeQuery conn is " + connection.toString());
    return new tinySQLResultSet(connection.executetinySQL(this), this);

  }
  public synchronized ResultSet executeQuery(String sql)
       throws SQLException {

    // tinySQL only supports one result set at a time, so
    // don't let them get another one, just in case it's
    // hanging out.
    //
    result = null; 
    statementString = sql;

    // create a new tinySQLResultSet with the tsResultSet
    // returned from connection.executetinySQL()
    //
    if ( debug) System.out.println("executeQuery conn is " + connection.toString());
    return new tinySQLResultSet(connection.executetinySQL(this), this);

  }

  /**
   * 
   * Execute an update, insert, delete, create table, etc. This can
   * be anything that doesn't return rows.
   * @see java.sql.PreparedStatement#executeUpdate
   * @exception java.sql.SQLException thrown when an error occurs executing
   * the SQL
   * @return either the row count for INSERT, UPDATE or DELETE or 0 for SQL statements that return nothing
   */
  public synchronized int executeUpdate(String sql) throws SQLException {

    statementString = sql;
    return connection.executetinyUpdate(this);

  }
  public synchronized int executeUpdate() throws SQLException {

    return connection.executetinyUpdate(this);

  }

  /**
   * 
   * Executes some SQL and returns true or false, depending on
   * the success. The result set is stored in result, and can
   * be retrieved with getResultSet();
   * @see java.sql.PreparedStatement#execute
   * @exception SQLException raised for any errors
   * @param sql the SQL to be executed
   * @return true if there is a result set available
   */
  public boolean execute() throws SQLException {

    // a result set object
    //
    tsResultSet r;

    // execute the query 
    //
    r = connection.executetinySQL(this);

    // check for a null result set. If it wasn't null,
    // use it to create a tinySQLResultSet, and return whether or
    // not it is null (not null returns true).
    //
    if( r == null ) {
      result = null;
    } else {
      result = new tinySQLResultSet(r, this);
    }
    return (result != null);

  }
  public boolean execute(String sql) throws SQLException {

    // a result set object
    //
    tsResultSet r;
    statementString = sql;

    // execute the query 
    //
    r = connection.executetinySQL(this);

    // check for a null result set. If it wasn't null,
    // use it to create a tinySQLResultSet, and return whether or
    // not it is null (not null returns true).
    //
    if( r == null ) {
      result = null;
    } else {
      result = new tinySQLResultSet(r, this);
    }
    return (result != null);

  }

  /**
   * Returns the current query-String 
   */
  public String getSQLString ()
  {
  	return statementString;
  }

  /**
   * 
   * Close any result sets. This is not used by tinySQL.
   * @see java.sql.PreparedStatement#close
   *
   */
  public void close() throws SQLException
  {
     int i;
     tinySQLTable nextTable;
     for ( i = 0; i < tableList.size(); i++ )
     {
        nextTable = (tinySQLTable)tableList.elementAt(i);
        if ( debug ) System.out.println("Closing " + nextTable.table);
        nextTable.close();
     }
  }

  /**
   * 
   * Returns the last result set
   * @see java.sql.PreparedStatement#getResultSet
   * @return null if no result set is available, otherwise a result set
   *
   */
  public ResultSet getResultSet() throws SQLException {

    ResultSet r;

    r = result;    // save the existing result set
    result = null; // null out the existing result set
    return r;      // return the previously extant result set
  }

  /**
   * 
   * Return the row count of the last operation. tinySQL does not support
   * this, so it returns -1
   * @see java.sql.PreparedStatement#getUpdateCount
   * @return -1
   */
  public int getUpdateCount() throws SQLException {
    return -1;
  }

  /**
   *
   * This returns true if there are any pending result sets. This
   * should only be true after invoking execute() 
   * @see java.sql.PreparedStatement#getMoreResults
   * @return true if rows are to be gotten
   *
   */
  public boolean getMoreResults() throws SQLException {

    return (result != null);

  }

  /**
   *
   * Get the maximum field size to return in a result set.
   * @see java.sql.PreparedStatement#getMaxFieldSize
   * @return the value of max field size
   *
   */
  public int getMaxFieldSize() throws SQLException {
    return max_field_size;
  }

  /**
   *
   * set the max field size.
   * @see java.sql.PreparedStatement#setMaxFieldSize
   * @param max the maximum field size
   *
   */
  public void setMaxFieldSize(int max) throws SQLException {
    max_field_size = max;
  }

  /**
   * 
   * Get the maximum row count that can be returned by a result set.
   * @see java.sql.PreparedStatement#getMaxRows
   * @return the maximum rows 
   *
   */
  public int getMaxRows() throws SQLException {
    return max_rows;
  }

  /**
   *
   * Get the maximum row count that can be returned by a result set.
   * @see java.sql.PreparedStatement.setMaxRows
   * @param max the max rows
   *
   */
  public void setMaxRows(int max) throws SQLException {
    max_rows = max;
  }

  /**
   *
   * If escape scanning is on (the default) the driver will do
   * escape substitution before sending the SQL to the database.
   * @see java.sql.PreparedStatement#setEscapeProcessing
   * @param enable this does nothing right now
   *
   */
  public void setEscapeProcessing(boolean enable)
       throws SQLException {
    throw new SQLException("The tinySQL Driver doesn't " +
                           "support escape processing.");
  }

  /**
   *
   * Discover the query timeout.
   * @see java.sql.PreparedStatement#getQueryTimeout
   * @see setQueryTimeout
   * @return the timeout value for this statement
   *
   */
  public int getQueryTimeout() throws SQLException {
    return timeout;
  }

  /**
   *
   * Set the query timeout.
   * @see java.sql.PreparedStatement#setQueryTimeout
   * @see getQueryTimeout
   * @param x the new query timeout value
   *
   */
  public void setQueryTimeout(int x) throws SQLException {
    timeout = x;
  }

  /**
   *
   * This can be used by another thread to cancel a statement. This
   * doesn't matter for tinySQL, as far as I can tell.
   * @see java.sql.PreparedStatement#cancel
   *
   */
  public void cancel() {
  }

  /**
   *
   * Get the warning chain associated with this Statement
   * @see java.sql.PreparedStatement#getWarnings
   * @return the chain of warnings
   *
   */
  public final SQLWarning getWarnings() throws SQLException {
    return null;
  }

  /**
   *
   * Clear the warning chain associated with this Statement
   * @see java.sql.PreparedStatement#clearWarnings
   *
   */
  public void clearWarnings() throws SQLException {
  }

  /**
   * 
   * Sets the cursor name for this connection. Presently unsupported.
   *
   */
  public void setCursorName(String unused) throws SQLException {
    throw new SQLException("tinySQL does not support cursors.");
  }

    //--------------------------JDBC 2.0-----------------------------


    /**
     * JDBC 2.0
     *
     * Gives the driver a hint as to the direction in which
         * the rows in a result set
     * will be processed. The hint applies only to result sets created 
     * using this Statement object.  The default value is 
     * ResultSet.FETCH_FORWARD.
     * <p>Note that this method sets the default fetch direction for 
         * result sets generated by this <code>Statement</code> object.
         * Each result set has its own methods for getting and setting
         * its own fetch direction.
     * @param direction the initial direction for processing rows
     * @exception SQLException if a database access error occurs
         * or the given direction
     * is not one of ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, or
     * ResultSet.FETCH_UNKNOWN
     */
    public void setFetchDirection(int direction) throws SQLException {
      throw new SQLException("tinySQL does not support setFetchDirection.");
    }

    /**
     * JDBC 2.0
     *
     * Retrieves the direction for fetching rows from
         * database tables that is the default for result sets
         * generated from this <code>Statement</code> object.
         * If this <code>Statement</code> object has not set
         * a fetch direction by calling the method <code>setFetchDirection</code>,
         * the return value is implementation-specific.
     *
     * @return the default fetch direction for result sets generated
         *          from this <code>Statement</code> object
     * @exception SQLException if a database access error occurs
     */
    public int getFetchDirection() throws SQLException {
      throw new SQLException("tinySQL does not support getFetchDirection.");
    }

    /**
     * JDBC 2.0
     *
     * Gives the JDBC driver a hint as to the number of rows that should 
     * be fetched from the database when more rows are needed.  The number 
     * of rows specified affects only result sets created using this 
     * statement. If the value specified is zero, then the hint is ignored.
     * The default value is zero.
     *
     * @param rows the number of rows to fetch
     * @exception SQLException if a database access error occurs, or the
     * condition 0 <= rows <= this.getMaxRows() is not satisfied.
     */
    public void setFetchSize(int rows) throws SQLException {
      if ((rows <= 0) || (rows >= this.getMaxRows ()))
    		  throw new SQLException ("Condition 0 <= rows <= this.getMaxRows() is not satisfied");
    
      fetchsize = rows;  
    }

    /**
     * JDBC 2.0
     *
     * Retrieves the number of result set rows that is the default 
         * fetch size for result sets
         * generated from this <code>Statement</code> object.
         * If this <code>Statement</code> object has not set
         * a fetch size by calling the method <code>setFetchSize</code>,
         * the return value is implementation-specific.
     * @return the default fetch size for result sets generated
         *          from this <code>Statement</code> object
     * @exception SQLException if a database access error occurs
     */
    public int getFetchSize() throws SQLException {
      return fetchsize;
    }

    /**
     * JDBC 2.0
     *
     * Retrieves the result set concurrency.
     */
    public int getResultSetConcurrency() throws SQLException {
      throw new SQLException("tinySQL does not support ResultSet concurrency.");
    }

    /**
     * JDBC 2.0
     *
     * Determine the result set type.
     */
    public int getResultSetType()  throws SQLException {
      throw new SQLException("tinySQL does not support getResultSetType.");
    }

    /**
     * JDBC 2.0
     *
     * Adds a SQL command to the current batch of commmands for the statement.
     * This method is optional.
     *
     * @param sql typically this is a static SQL INSERT or UPDATE statement
     * @exception SQLException if a database access error occurs, or the
     * driver does not support batch statements
     */
    public void addBatch() throws SQLException {
      throw new SQLException("tinySQL does not support addBatch.");
    }
    public void addBatch( String sql ) throws SQLException {
      throw new SQLException("tinySQL does not support addBatch.");
    }

    /**
     * JDBC 2.0
     *
     * Makes the set of commands in the current batch empty.
     * This method is optional.
     *
     * @exception SQLException if a database access error occurs or the
     * driver does not support batch statements
     */
    public void clearBatch() throws SQLException {
      throw new SQLException("tinySQL does not support clearBatch.");
    }

    /**
     * JDBC 2.0
     * 
     * Submits a batch of commands to the database for execution.
     * This method is optional.
     *
     * @return an array of update counts containing one element for each
     * command in the batch.  The array is ordered according 
     * to the order in which commands were inserted into the batch.
     * @exception SQLException if a database access error occurs or the
     * driver does not support batch statements
     */
    public int[] executeBatch() throws SQLException {
      throw new SQLException("tinySQL does not support executeBatch.");
    }

    /**
     * JDBC 2.0
     * 
     * Returns the <code>Connection</code> object
         * that produced this <code>Statement</code> object.
         * @return the connection that produced this statement
     * @exception SQLException if a database access error occurs
     */
    public Connection getConnection()  throws SQLException
    {
      return connection;
    }
/*
 *  Set methods for the prepared statement.
 */
    public void setBoolean(int parameterIndex,boolean inputValue)
       throws SQLException
    {
       if ( inputValue ) setString(parameterIndex,"TRUE");
       else setString(parameterIndex,"FALSE");
    }
    public void setInt(int parameterIndex,int inputValue)
       throws SQLException
    {
       setString(parameterIndex,Integer.toString(inputValue));
    }
    public void setDouble(int parameterIndex,double inputValue)
       throws SQLException
    {
       setString(parameterIndex,Double.toString(inputValue));
    }
    public void setBigDecimal(int parameterIndex,BigDecimal inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setDate(int parameterIndex,java.sql.Date inputValue,
       java.util.Calendar inputCalendar) throws SQLException
    {
       String dateString;
/*
 *     Convert string to YYYYMMDD format that dBase needs.
 */
       if ( inputValue == (java.sql.Date)null )
       {
          setString(parameterIndex,(String)null);
       } else if ( inputValue.toString().trim().length() < 8 ) {
          setString(parameterIndex,(String)null);
       } else {
          dateString = inputValue.toString().trim();
/*
 *        Convert date string to the standard YYYYMMDD format
 */
          dateString = UtilString.dateValue(dateString);
          setString(parameterIndex,dateString);
       }
    }
    public void setDate(int parameterIndex,java.sql.Date inputValue)
       throws SQLException
    {
       String dateString;
/*
 *     Convert string to YYYYMMDD format that dBase needs.
 */
       dateString = UtilString.dateValue(inputValue.toString());
       setString(parameterIndex,dateString);
    }
    public void setTime(int parameterIndex,java.sql.Time inputValue,
       java.util.Calendar inputCalendar) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setTime(int parameterIndex,java.sql.Time inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setTimestamp(int parameterIndex,java.sql.Timestamp inputValue,
       java.util.Calendar inputCalendar ) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setTimestamp(int parameterIndex,java.sql.Timestamp inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setAsciiStream(int parameterIndex,
       java.io.InputStream inputValue,int streamLength) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setUnicodeStream(int parameterIndex,
       java.io.InputStream inputValue,int streamLength) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setBinaryStream(int parameterIndex,
       java.io.InputStream inputValue,int streamLength) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setCharacterStream(int parameterIndex,
       java.io.Reader inputValue,int streamLength) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setRef(int parameterIndex,java.sql.Ref inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setBlob(int parameterIndex,java.sql.Blob inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setArray(int parameterIndex,java.sql.Array inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setClob(int parameterIndex,java.sql.Clob inputValue)
       throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setByte(int parameterIndex,byte inputValue)
       throws SQLException
    {
       setString(parameterIndex,Byte.toString(inputValue));
    }
    public void setBytes(int parameterIndex,byte[] inputValue)
       throws SQLException
    {
       setString(parameterIndex,Byte.toString(inputValue[0]));
    }
    public void setShort(int parameterIndex,short inputValue)
       throws SQLException
    {
       setString(parameterIndex,Short.toString(inputValue));
    }
    public void setFloat(int parameterIndex,float inputValue)
       throws SQLException
    {
       setString(parameterIndex,Float.toString(inputValue));
    }
    public void setLong(int parameterIndex,long inputValue)
       throws SQLException
    {
       setString(parameterIndex,Long.toString(inputValue));
    }
    public void setObject(int parameterIndex,Object inputValue)
       throws SQLException
    {
       setObject(parameterIndex,inputValue,0,0);
    }
    public void setObject(int parameterIndex,Object inputValue,
       int targetSQLType) throws SQLException
    {
       setObject(parameterIndex,inputValue,targetSQLType,0);
    }
    public void setObject(int parameterIndex,Object inputValue,
       int targetSQLType, int scale) throws SQLException
    {
       setString(parameterIndex,inputValue.toString());
    }
    public void setNull(int parameterIndex,int sqlType)
       throws SQLException
    {
       setNull(parameterIndex,sqlType,(String)null);
    }
    public void setNull(int parameterIndex,int sqlType,String sqlTypeName)
       throws SQLException
    {
       if ( parameterIndex > substitute.size() )
          throw new SQLException("Parameter index " + parameterIndex 
          + invalidIndex);
       substitute.setElementAt((String)null,parameterIndex-1);
    }
    public void setString( int parameterIndex, String setString)
       throws SQLException
    {
       if ( parameterIndex > substitute.size() )
          throw new SQLException("Parameter index " + parameterIndex 
          + invalidIndex);
       substitute.setElementAt(setString,parameterIndex-1);
    }
    public void clearParameters() throws SQLException
    {
       substitute.removeAllElements();
    }
/*
 *  Update the actions based upon the contents of the substitute Vector.
 *  Only INSERT and UPDATE commands are supported at this time.
 */
    public void updateActions(Vector inputActions) throws SQLException
    {
       Vector values,originalValues;
       Hashtable action;
       String actionType,valueString;
       int i,j,subCount;
       if ( actions == (Vector)null )
          actions = inputActions;
       if ( actions == (Vector)null ) return;
       for ( i = 0; i < actions.size(); i++ )
       {
          action = (Hashtable)actions.elementAt(i); 
          actionType = (String)action.get("TYPE");
          if ( actionType.equals("INSERT") | actionType.equals("UPDATE") )
          {
/*
 *           Look for the original values (with the ? for parameters).
 */
             originalValues = (Vector)action.get("ORIGINAL_VALUES");
             values = (Vector)action.get("VALUES");
             if ( originalValues == (Vector)null ) 
             {
                originalValues = (Vector)values.clone();
                action.put("ORIGINAL_VALUES",originalValues);
             }
             subCount = 0;
             for ( j = 0; j < originalValues.size(); j++ )
             {
                valueString = (String)originalValues.elementAt(j);
                if ( valueString.equals("?") )
                {
                   if ( subCount > substitute.size() - 1 )
                      throw new SQLException("Substitution index " + subCount
                      + " not between 0 and " 
                      + Integer.toString(substitute.size() - 1));
                   values.setElementAt(substitute.elementAt(subCount),j);
                   subCount++;
                }
             }
          }
       }
    }
    public void addTable(tinySQLTable inputTable)
    {
       int i;
       tinySQLTable nextTable;
       for ( i = 0; i < tableList.size(); i++ )
       {
          nextTable = (tinySQLTable)tableList.elementAt(i);
          if ( nextTable.table.equals(inputTable.table) ) return;
       }
       tableList.addElement(inputTable);
    }
      
     
    public Vector getActions()
    {
       return actions;
    }
    public ResultSetMetaData getMetaData()
    {
       return (ResultSetMetaData)null;
    }
       
}
