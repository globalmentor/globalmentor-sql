package com.garretwilson.sql;

import java.util.*;
import java.sql.*;
import javax.sql.*;
import com.garretwilson.util.*;

/**Facade pattern for accessing a table through SQL and JDBC.
	<p>Classes that extend this class should implement the following methods:</p>
	<ul>
	  <li><code>public void insert(<em>ObjectType</em>)</code></li>
		<li><code>protected Object retrieve(ResultSet)</code></li>
	  <li><code>public void update(final String primaryKeyValue, <em>ObjectType</em>)</code></li>
	</ul>
@author Garret Wilson
*/
public abstract class Table implements SQLConstants
{

	/**The data source that allows access to the database.*/
	private final DataSource dataSource;

		/**@return The data source that allows access to the database.*/
		protected DataSource getDataSource() {return dataSource;}

	/**The name of the table.*/
	private final String name;

		/**@return The name of the table.*/
		public String getName() {return name;}

	/**The SQL definition string for the table, suitable for an "SQL CREATE
		TABLE tableName <em>definition</em>" statement.
	*/
	private final String definition;

		/**@return The SQL definition string for the table.*/
		protected String getDefinition() {return definition;}

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
	@param definition The SQL definition string for the table, suitable for an
		"SQL CREATE TABLE tableName <em>definition</em>" statement.
	@param primaryKey The name of the primiary key column.
	*/
	public Table(final DataSource dataSource, final String name, final String definition, final String primaryKey)
	{
		this(dataSource, name, definition, primaryKey, null); //construct the object with no default ordering
	}

	/**Constructor.
	@param dataSource The connection factory.
	@param name The name of the table.
	@param definition The SQL definition string for the table, suitable for an
		"SQL CREATE TABLE tableName <em>definition</em>" statement.
	@param primaryKey The name of the primiary key column.
	@param defaultOrderBy The name of the default ordering column(s).
	*/
	public Table(final DataSource dataSource, final String name, final String definition, final String primaryKey, final String defaultOrderBy)
	{
		this.dataSource=dataSource; //set the data source
		this.name=name; //set the name
		this.definition=definition; //set the definition
		this.primaryKey=primaryKey; //save the primary key column name
		this.defaultOrderBy=defaultOrderBy; //save the default ordering
	}

	/**@return A text definition suitable for an SQL CREATE TABLE XXX (definition)
		statement.*/
//G***del	public String getSQLTableDefinition();

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
				  SQLUtilities.dropTable(statement, getName(), true);	//remove the table if it exists
				}
				SQLUtilities.createTable(statement, getName(), getDefinition());	//create the table
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
					return resultSet.getRow();  //return the row number, which will be the number of rows in the table
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
	protected abstract Object retrieve(final ResultSet resultSet) throws SQLException;

	/**Selects all the records from the table using the given criteria with the
		default ordering.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList select(final String expression) throws SQLException
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
	public SubList select(final String expression, final int startIndex, final int count) throws SQLException
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
	public SubList select(final String expression, final String orderBy) throws SQLException
	{
		return select(expression, 0, Integer.MAX_VALUE, orderBy);  //return all the rows we can find, starting at the first
	}

	/**Selects records from the table using the given criteria.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The name of the column on which to sort, or
		<code>null</code> if the default ordering should be used.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList select(final String expression, final int startIndex, final int count, String orderBy) throws SQLException
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
				statementStringBuffer.append(SELECT).append(' ').append(WILDCARD_CHAR); //append "SELECT *"
				statementStringBuffer.append(' ').append(FROM).append(' ').append(getName()); //append " FROM name"
				if(expression!=null)  //if a valid expression was given
				  statementStringBuffer.append(' ').append(WHERE).append(' ').append(expression); //append " WHERE expression"
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
					final ArraySubList list=new ArraySubList();	//create a list of results
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
	public SubList selectAll() throws SQLException
	{
		return selectAll(null);  //select all records using the default ordering
	}

	/**Selects all records from the table within a given range.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList selectAll(final int startIndex, final int count) throws SQLException
	{
		return selectAll(startIndex, count, null);  //select all records using the default ordering
	}

	/**Selects all records from the table.
	@param orderBy The name of the column on which to sort.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList selectAll(final String orderBy) throws SQLException
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
	public SubList selectAll(final int startIndex, final int count, final String orderBy) throws SQLException
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
	public SubList selectColumn(final String columnName, final String columnValue) throws SQLException
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
	public Object selectColumnRecord(final String columnName, final String columnValue) throws SQLException
	{
		final SubList recordList=selectColumn(columnName, columnValue); //get all the matching records
		return recordList.size()>0 ? recordList.get(0) : null;  //if there are records, return the first record we retrieved; otherwise, return null
	}

	/**Selects a single record by its primary key column.
	@param primaryKeyValue The value of the primary key of the record to return.
	@return The object the primary key of which matches the given value, or
		<code>null</code> if no record matches.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public Object selectByPrimaryKey(final String primaryKeyValue) throws SQLException
	{
		return selectColumnRecord(getPrimaryKey(), primaryKeyValue);  //select the record based upon the primary key
	}

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

}
