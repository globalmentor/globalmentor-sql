package com.garretwilson.sql;

import java.sql.*;
import javax.sql.*;

import com.garretwilson.text.CharacterConstants;
import com.garretwilson.util.*;

/**Facade pattern for accessing a table through SQL and JDBC.
<p>Classes that extend this class must implement the following methods:</p>
<ul>
  <li><code>public void insert(<var>T</var>)</code></li>
	<li><code>protected <var>T</var> retrieve(ResultSet)</code></li>
  <li><code>public void update(final String primaryKeyValue, <var>T</var>)</code></li>
</ul>
<p>This class has the capability of caching database record count, but defaults
	to a cache that is always expired.</p>
@author Garret Wilson
*/
public abstract class Table<T> implements SQLConstants, CharacterConstants
{

	/**The SQL wildcard ('*') character in string format.*/
	protected final static String WILDCARD_STRING=String.valueOf(WILDCARD_CHAR);
	
	/**The data source that allows access to the database.*/
	private final DataSource dataSource;

		/**@return The data source that allows access to the database.*/
		protected DataSource getDataSource() {return dataSource;}

	/**The name of the table.*/
	private final String name;

		/**@return The name of the table.*/
		public String getName() {return name;}

	/**The definition of the table.*/
	private final ColumnDefinition[] definition;

		/**@return The definition of the table.*/
		protected ColumnDefinition[] getDefinition() {return definition;}

	/**The name of the table primary key column.*/
	private final String primaryKey;

		/**@return The name of the table primary key column.*/
		public String getPrimaryKey() {return primaryKey;}

	/**The name of the default ordering column(s).*/
	private final String defaultOrderBy;

		/**@return The name of the default ordering column(s).*/
		public String getDefaultOrderBy() {return defaultOrderBy;}

	/**Default order constructor.
	@param dataSource The connection factory.
	@param name The name of the table.
	@param definition The definition of the table.
	@param primaryKey The name of the primiary key column.
	*/
	public Table(final DataSource dataSource, final String name, final ColumnDefinition[] definition, final String primaryKey)
	{
		this(dataSource, name, definition, primaryKey, null); //construct the object with no default ordering
	}

	/**Constructor.
	@param dataSource The connection factory.
	@param name The name of the table.
	@param definition The definition of the table.
	@param primaryKey The name of the primiary key column.
	@param defaultOrderBy The name of the default ordering column(s).
	*/
	public Table(final DataSource dataSource, final String name, final ColumnDefinition[] definition, final String primaryKey, final String defaultOrderBy)
	{
		this.dataSource=dataSource; //set the data source
		this.name=name; //set the name
		this.definition=definition; //set the definition
		this.primaryKey=primaryKey; //save the primary key column name
		this.defaultOrderBy=defaultOrderBy; //save the default ordering
	}

	/**@return A text definition suitable for an SQL CREATE TABLE <var>table</var>
	 	<var>definition</var> statement.
	@see #getDefinition()
	*/
	protected String getSQLDefinition()
	{
		final StringBuilder stringBuilder=new StringBuilder();	//we'll accumulate the SQL definition here
		final ColumnDefinition[] columns=getDefinition();	//get the column definitions
		for(ColumnDefinition column:columns)	//look at each column definition
		{
				//append the column name and type
			stringBuilder.append(column.getName()).append(SPACE_CHAR).append(column.getType());
			if(column.isPrimaryKey())	//if this columns is a primary key
			{
				stringBuilder.append(SPACE_CHAR).append(PRIMARY_KEY);	//append the primary key designation
			}
			stringBuilder.append(LIST_SEPARATOR).append(SPACE_CHAR);	//append a list separator and a space
		}
		stringBuilder.delete(stringBuilder.length()-2, stringBuilder.length());	//remove the last two characters, which together is a useless list delimiter sequence
		return stringBuilder.toString();	//return the SQL definition string we constructed
	}
	
	/**Creates the table after first deleting it if it already exists.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public void create() throws SQLException
	{
		create(true); //create the table after first deleting it
	}

	/**Creates the table.
	@param drop Whether the table should first be deleted if it exists.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public void create(final boolean drop) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				if(drop)  //if we should first drop the table
				{
					drop(true);	//remove the table if it exists
				}
				SQLUtilities.createTable(statement, getName(), getSQLDefinition());	//create the table
				invalidateCachedRecordCount();	//any cached record count is no longer valid
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
	}

	/**Deletes one or more rows from the table that fit the given criteria.
	@param expression The SQL expression that deletes the records, or
		<code>null</code> if all records should be deleted.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public void delete(final String expression) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				SQLUtilities.delete(statement, getName(), expression);	//delete the records
				invalidateCachedRecordCount();	//any cached record count is no longer valid
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
	}

	/**Deletes rows from the table for which the given column contains
		the specified value.
	@param columnName The name of the column to test.
	@param columnValue The column value necessary for a record to be included.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public void deleteColumn(final String columnName, final String columnValue) throws SQLException
	{
		delete(columnName+EQUALS+SINGLE_QUOTE+columnValue+SINGLE_QUOTE);  //delete the records which have the correct value for this column
	}

	/**Deletes records by primary key column.
	@param primaryKeyValue The value of the primary key of the record to delete.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public void deleteByPrimaryKey(final String primaryKeyValue) throws SQLException
	{
		deleteColumn(getPrimaryKey(), primaryKeyValue);  //delete the record based upon the primary key
	}

	/**Drops (deletes) the table if it exists.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public void drop() throws SQLException
	{
		drop(true); //drop the table, checking for existence
	}

	/**Drops (deletes) the table.
	@param ifExists <code>true</code> if the the table should checked to exist
		before deleting.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public void drop(final boolean ifExists) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				SQLUtilities.dropTable(statement, getName(), ifExists);	//remove the table if it exists
				invalidateCachedRecordCount();	//any cached record count is no longer valid
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
	}

	/**@return The number of rows in the table.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public int getRecordCount() throws SQLException
	{
			//if we have a valid cached record count, and it hasn't expired, yet 
		if(getCachedRecordCount()>=0 && getLastCacheTime()>System.currentTimeMillis()-getCachedRecordCountLifetime())
		{
			return getCachedRecordCount();	//return the cached record count
		}
		else	//if our cached record count is expired or invalid
		{
			final Connection connection=getDataSource().getConnection();	//get a connection to the database
			try
			{
					//create a statement that can quickly scroll to the end
				final Statement statement=connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
				try
				{
						//create a statement for selecting all the records: "SELECT * FROM table"
					final StringBuffer statementStringBuffer=new StringBuffer();  //create a string buffer in which to construct the statement
					statementStringBuffer.append(SELECT).append(' ').append(WILDCARD_CHAR); //append "SELECT *"
					statementStringBuffer.append(' ').append(FROM).append(' ').append(getName()); //append " FROM name"
					final ResultSet resultSet=statement.executeQuery(statementStringBuffer.toString()); //select all the records
					try
					{
						resultSet.last();  //move to after the last row G***make this a comvenience method that knows how to iterate the table of ResultSet.last() isn't supported
						final int recordCount=resultSet.getRow();  //get the row number, which will be the number of rows in the table
						setCachedRecordCount(recordCount);	//update the cached record count
						return recordCount;	//return the record count

					}
					finally
					{
						resultSet.close();	//always close the result set
					}
				}
				finally
				{
					statement.close();	//always close the statement
				}
			}
			finally
			{
				connection.close();	//always close the connection
			}
		}
	}

	/**Inserts an object into the table.
	@param object The object to insert.
	@exception SQLException Thrown if there is an error accessing the table.
	*/
	public abstract void insert(final T object) throws SQLException;

	/**Inserts values into the table.
	@param values The array of values to insert into the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	protected void insert(final Object[] values) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				SQLUtilities.insertValues(statement, getName(), values);	//insert the values into the table
				invalidateCachedRecordCount();	//any cached record count is no longer valid
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
	}

	/**Creates a new object and retrieves its contents from the current row of
		the given result set.
	@param resultSet The result set that contains the object information.
	@return A new object with information from the current row in the result set.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public abstract T retrieve(final ResultSet resultSet) throws SQLException;

	/**Determines if a table exists in the database.
	@return <code>true</code> if the table exists, else <code>false</code>.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public boolean exists() throws SQLException
	{
		//This function uses a brute-force method of checking for table existence:
		//trying to access the first row in the table, and assuming that any error
		//means the table does not exist.
		//TODO find a better way to check for table existence, such as looking at database metadata, if there is a standard JDBC way to do that
		try	
		{
			select("TOP 1 *", null, 0, Integer.MAX_VALUE, null);  //select the first record from the database TODO use constants, and create a convenience routine for selectExpression methods
			return true;	//if we can select records from the table, the table exists
		}
		catch(SQLException sqlException)	//if there is any error
		{
			return false;	//assume the table doesn't exist
		}
	}

	/**Selects all the records from the table using the given criteria with the
		default ordering.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String expression) throws SQLException
	{
		return select(expression, 0, Integer.MAX_VALUE);  //return all the rows we can find, starting at the first
	}

	/**Selects records from the table using the given criteria with the default
		ordering.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String expression, final int startIndex, final int count) throws SQLException
	{
		return select(expression, startIndex, count, null); //select all the rows within the given range using the default ordering
	}

	/**Selects all the records from the table using the given criteria, sorting
		on the given column.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param orderBy The name of the column on which to sort.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String expression, final String orderBy) throws SQLException
	{
		return select(expression, 0, Integer.MAX_VALUE, orderBy);  //return all the rows we can find, starting at the first
	}

	/**Selects all columns of records from the table using the given criteria.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The name of the column on which to sort, or
		<code>null</code> if the default ordering should be used.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String whereExpression, final int startIndex, final int count, String orderBy) throws SQLException
	{
		return select(WILDCARD_STRING, whereExpression, startIndex, count, orderBy);	//select all columns
	}
	/**Selects records from the table using the given criteria.
	@param selectExpression The SQL expression that selects the columns.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The name of the column on which to sort, or
		<code>null</code> if the default ordering should be used.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String selectExpression, final String whereExpression, final int startIndex, final int count, String orderBy) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
				//create a statement that can quickly scroll to the end
			final Statement statement=connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
//G***del			final Statement statement=connection.createStatement(); //create a statement
			try
			{
					//create a statement for selecting the records: "SELECT * FROM table WHERE expression"
				final StringBuffer statementStringBuffer=new StringBuffer();  //create a string buffer in which to construct the statement
				statementStringBuffer.append(SELECT).append(' ').append(selectExpression); //append "SELECT <var>selectExpression</var>"
				statementStringBuffer.append(' ').append(FROM).append(' ').append(getName()); //append " FROM name"
				if(whereExpression!=null && whereExpression.length()>0)  //if a valid expression was given
				  statementStringBuffer.append(' ').append(WHERE).append(' ').append(whereExpression); //append " WHERE <var>whereExpression</var>"
				if(orderBy==null) //if no ordering was given
				{
					orderBy=getDefaultOrderBy();  //use the default ordering
				}
				if(orderBy!=null)  //if we have an ordering
				{
				  statementStringBuffer.append(' ').append(ORDER_BY).append(' ').append(orderBy); //append " ORDER BY orderBy"
				}
				final ResultSet resultSet=statement.executeQuery(statementStringBuffer.toString()); //select the records
				try
				{
					final ArraySubList<T> list=new ArraySubList<T>();	//create a list of results
					list.setStartIndex(startIndex); //show for what index we're returning results
					final int startRow=startIndex+1; //we'll start at the requested row
					final int endRow=count<Integer.MAX_VALUE ? startRow+count : Integer.MAX_VALUE; //we'll end when we get past the requested count (allowing for a requested maximum amount)
				  int row=startRow; //we'll start at the starting row
					resultSet.absolute(startRow); //go to the starting row
					if(!resultSet.isBeforeFirst() && !resultSet.isAfterLast())  //if there are rows, and we haven't gone past all the rows
					{
						while(row<endRow) //while we're not past the ending row
						{
							list.add(retrieve(resultSet)); //retrieve the object from the row and add it to our list
							if(resultSet.next())  //if there is another row
							{
								++row;  //show that we just went to the next row
							}
							else  //if there are no more rows
							{
								break;  //stop retrieving rows
							}
						}
					}
					resultSet.last();  //move to the last row
					final int superListSize=resultSet.getRow();  //get the row number, which will be the number of rows in the table
				  list.setSuperListSize(superListSize);  //show how many rows we found
				  return list;	//return the list of objects representing the records we found
/*G***del

					while(resultSet.next())	//while we have items left in our result set
					{
						++rowCount;  //show that we're going to the next row
						list.add(retrieve(resultSet)); //retrieve the object from the row and add it to our list
					}
*/
				}
				finally
				{
					resultSet.close();	//always close the result set
				}
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
//G***del		return list;	//return the list of objects representing the records we found
	}

	/**Selects all records from the table.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll() throws SQLException
	{
		return selectAll(null);  //select all records using the default ordering
	}

	/**Selects all records from the table within a given range.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final int startIndex, final int count) throws SQLException
	{
		return selectAll(startIndex, count, null);  //select all records using the default ordering
	}

	/**Selects all records from the table.
	@param orderBy The name of the column on which to sort.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final String orderBy) throws SQLException
	{
		return select(null, orderBy);  //select all records using the given ordering
	}

	/**Selects all records from the table within a given range.
	@param orderBy The name of the column on which to sort.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final int startIndex, final int count, final String orderBy) throws SQLException
	{
		return select(null, startIndex, count, orderBy);  //select all records using the given ordering
	}

	/**Selects all the records from the table for which the given column contains
		the specified value.
	@param columnName The name of the column to test.
	@param columnValue The column value necessary for a record to be included.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectColumn(final String columnName, final String columnValue) throws SQLException
	{
		return select(columnName+EQUALS+SINGLE_QUOTE+columnValue+SINGLE_QUOTE);  //select the records which have the correct value for this column
	}

	/**Selects a single record from the table for which the given column contains
		the specified value.
	@param columnName The name of the column to test.
	@param columnValue The column value necessary for a record to be included.
	@return The first object that matches the given criteria, or <code>null</code>
		if no record matches.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public T selectColumnRecord(final String columnName, final String columnValue) throws SQLException
	{
		final SubList<T> recordList=selectColumn(columnName, columnValue); //get all the matching records
		return recordList.size()>0 ? recordList.get(0) : null;  //if there are records, return the first record we retrieved; otherwise, return null
	}

	/**Selects a single record by its primary key column.
	@param primaryKeyValue The value of the primary key of the record to return.
	@return The object the primary key of which matches the given value, or
		<code>null</code> if no record matches.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public T selectByPrimaryKey(final String primaryKeyValue) throws SQLException
	{
		return selectColumnRecord(getPrimaryKey(), primaryKeyValue);  //select the record based upon the primary key
	}

	/**Updates a user in the database table.
	@param primaryKeyValue The value of the primary key for which record to update.
	@param object The new information for the record.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public abstract void update(final String primaryKeyValue, final T object) throws SQLException;

	/**Inserts values into the table.
	@param primaryKeyValue The value of the primary key for which record to update.
	@param namesValues The array of name/value pair arrays to update.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	protected void update(final String primaryKeyValue, final Object[][] namesValues) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				  //update the values for the row with the primary key value
				SQLUtilities.updateTable(statement, getName(), namesValues, getPrimaryKey()+EQUALS+SINGLE_QUOTE+primaryKeyValue+SINGLE_QUOTE);
			}
			finally
			{
				statement.close();	//always close the statement
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
	}

	/**The number of milliseconds it takes before the cached record count will expire.*/
	private long cachedRecordCountLifetime=0;

		/**@return The number of milliseconds it takes before the cached record count will expire.*/
		public long getCachedRecordCountLifetime() {return cachedRecordCountLifetime;}

		/**Sets the number of milliseconds it takes before the cached record count will expire.
		@param lifetime The record count expiration, in milliseconds.
		*/
		public void setCachedRecordCountExpiration(final long lifetime)
		{
			cachedRecordCountLifetime=lifetime;	//update the record count lifetime
		}

	/**The cached record count, or -1 if invalid.*/
	private int cachedRecordCount=-1;

		/**@return The cached record count, or -1 if valid.*/
		protected int getCachedRecordCount() {return cachedRecordCount;}

		/**Sets the cached record count and updates the last cache time.
		@param recordCount The cached record count, or -1 if the cache should be
			invalidated.
		@see #setLastCacheTime
		*/
		private void setCachedRecordCount(final int recordCount)
		{
			cachedRecordCount=recordCount;	//update the cached record count
			setLastCacheTime(System.currentTimeMillis());  //show when we just updated the cache
		}

		/**Invalidates the cached record count, if any.
		@see #setCachedRecordCount
		*/
		public void invalidateCachedRecordCount()
		{
			setCachedRecordCount(-1);	//show that we don't have a valid cached record count
		}

	/**The last time the cache was updated, in milliseconds.*/
	private long lastCacheTime=0;

		/**@return The last time the cache was updated, in milliseconds.*/
		protected long getLastCacheTime() {return lastCacheTime;}

		/**Sets The last time the cache was updated.
		@param cacheTime The last time the cache was updated, in milliseconds.
		*/
		private void setLastCacheTime(final long cacheTime) {lastCacheTime=cacheTime;}

}
