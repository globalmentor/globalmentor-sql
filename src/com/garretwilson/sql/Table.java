package com.garretwilson.sql;

import java.sql.*;
import java.util.*;

import javax.sql.*;

import com.globalmentor.collections.ArraySubList;
import com.globalmentor.collections.SubList;
import com.globalmentor.model.NameValuePair;

import static com.garretwilson.sql.SQLConstants.*;
import static com.garretwilson.sql.SQLUtilities.*;
import static com.globalmentor.java.Characters.*;
import com.globalmentor.log.Log;

/**Facade pattern for accessing a table through SQL and JDBC.
<p>Classes that extend this class must implement the following methods:</p>
<ul>
  <li><code>public void insert(<var>T</var>)</code></li>
	<li><code>protected <var>T</var> retrieve(ResultSet)</code></li>
  <li><code>public void update(<var>T</var>, final String... primaryKeyValue)</code></li>
</ul>
<p>This class has the capability of caching database record count, but defaults
	to a cache that is always expired.</p>
@author Garret Wilson
*/
public abstract class Table<T> implements ResultSetObjectFactory<T>
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

	/**The definition of the table columns.*/
	private final Column<?>[] columns;

		/**@return The definition of the table columns.*/
		protected Column<?>[] getColumns() {return columns;}

	/**The table primary key columns, if any.*/
	private final Column<?>[] primaryKeys;

		/**@return The table primary key columns, if any.*/
		public Column<?>[] getPrimaryKeys() {return primaryKeys;}

		/**Sets the primary key column.
		@param primaryKey The primary key column.
		*/
//G***del		protected void setPrimaryKey(final Column primaryKey) {this.primaryKey=primaryKey;}

	/**The default ordering column(s).*/
	private Column<?>[] defaultOrderBy=new Column[]{};

		/**@return The default ordering column(s), empty if there is no default ordering.*/
		public Column<?>[] getDefaultOrderBy() {return defaultOrderBy;}

		/**Sets the default ordering.
		@param orderBy The default ordering column(s), if any.
		*/
		protected void setDefaultOrderBy(final Column<?>... orderBy) {this.defaultOrderBy=orderBy;}

	/**Constructor.
	@param dataSource The connection factory.
	@param name The name of the table.
	@param definition The definition of the table.
	*/
	public Table(final DataSource dataSource, final String name, final Column<?>... columns)
	{
		this.dataSource=dataSource; //set the data source
		this.name=name; //set the name
		this.columns=columns;	//save the columns
		final List<Column<?>> primaryKeyList=new ArrayList<Column<?>>(columns.length);	//create a list for colecting primary keys
		for(final Column<?> column:columns)	//look at each columns
		{
//G***del			column.setTable(this);	//associate the column with this table
			if(column.isPrimaryKey())	//if this columns is a primary key
			{
				primaryKeyList.add(column);	//add this column to our list of primary keys
			}
		}
		primaryKeys=primaryKeyList.toArray(new Column[primaryKeyList.size()]);	//store the primary keys
	}

	/**@return A text definition suitable for an SQL CREATE TABLE <var>table</var>
	 	<var>definition</var> statement.
	@see #getDefinition()
	*/
	protected String getSQLDefinition()
	{
		final StringBuilder stringBuilder=new StringBuilder();	//we'll accumulate the SQL definition here
		final Column<?>[] columns=getColumns();	//get the column definitions
//TODO del when works		final Column[] primaryKeys=getPrimaryKeys();	//get the primary keys
		for(int i=0; i<columns.length; ++i)	//look at each column definition
		{
			final Column<?> column=columns[i];	//look the current column			
			stringBuilder.append(column.getName()).append(SPACE_CHAR).append(getColumnSQLDefinition(column));	//add the column name and definition
/*TODO del when works
				//append the column name and type
			stringBuilder.append(column.getName()).append(SPACE_CHAR).append(column.getType());
			if(primaryKeys.length==1 && column.isPrimaryKey())	//if this columns is the only primary key
			{
				stringBuilder.append(SPACE_CHAR).append(PRIMARY_KEY);	//append the primary key designation
			}
*/
//TODO del; apparently duplicated in error			stringBuilder.append(getColumnSQLDefinition(column));	//add the definition for this column
			if(i<columns.length-1 || primaryKeys.length>1)	//if this isn't the last column, or there are primary keys to list
			{
				stringBuilder.append(LIST_SEPARATOR).append(SPACE_CHAR);	//append a list separator and a space
			}
		}
		if(primaryKeys.length>1)	//if there were more than one primary key
		{
			stringBuilder.append(PRIMARY_KEY).append('(');	//PRIMARY KEY(
			for(int i=0; i<primaryKeys.length; ++i)	//look at each primary key
			{
				final Column<?> primaryKey=primaryKeys[i];	//look the current primary key
				stringBuilder.append(primaryKey.getName());	//append the primary key
				if(i<primaryKeys.length-1)	//if this isn't the last primary key
				{
					stringBuilder.append(LIST_SEPARATOR).append(SPACE_CHAR);	//append a list separator and a space
				}
			}			
			stringBuilder.append(')');	//)
		}
		return stringBuilder.toString();	//return the SQL definition string we constructed
	}

	/**Determines the SQL definition for a particular column, not including the column name.
	@param column	The column for which a definition should be created
	@return An SQL definition of the column, not including the column name.
	*/ 
	protected String getColumnSQLDefinition(final Column<?> column)
	{
		final StringBuilder stringBuilder=new StringBuilder();	//create a new string builder for constructing the definition
		stringBuilder.append(column.getType());	//append the column type
		final Object defaultValue=column.getDefaultValue();	//get the column's default value
		if(defaultValue!=null)	//if the column has a default value
		{
			stringBuilder.append(SPACE_CHAR);
			stringBuilder.append(DEFAULT);	//DEFAULT
			stringBuilder.append(SPACE_CHAR);
			stringBuilder.append(SINGLE_QUOTE);	//'
			stringBuilder.append(createSQLValue(defaultValue.toString()));	//default value TODO create a method to automatically convert timestamps and the like
			stringBuilder.append(SINGLE_QUOTE);	//'
		}		
		if(getPrimaryKeys().length==1 && column.isPrimaryKey())	//if this column is the only primary key
		{
			stringBuilder.append(SPACE_CHAR).append(PRIMARY_KEY);	//append the primary key designation
		}
		return stringBuilder.toString();	//return the column SQL definition
	}
	
	/**Synchronizes the underlying table with this object's table definition by creating the table if needed
	 	and then adding or removing underlying table columns as needed.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public void synchronize() throws SQLException
	{
Log.trace("synchronizing table", getName());
		if(exists())	//if the table exists
		{
Log.trace("table exists");
			final Column<?>[] columns=getColumns();	//get our columns
			final Map<String, Column<?>> columnMap=new LinkedHashMap<String, Column<?>>(columns.length);	//create a map to hold our column definitions, keyed by name, maintaining the insertion order
			for(final Column<?> column:columns)	//for each column
			{
				columnMap.put(column.getName(), column);	//add this column to the map
			}
/*TODO del			
			final Set<Column> existingColumnSet=new HashSet<Column>(columns.length);	//create a set to hold our columns to check for existence
			addAll(existingColumnSet, columns);	//add all the columns to our set
			final Set<Column> columnSet=unmodifiableSet(new HashSet<Column>(existingColumnSet));	//create a separate set to hold our canonical list of columns			
*/
			final List<ColumnMetaData> columnMetaDataList=getColumnMetadata();	//get metadata describing the underlying table columns
			for(final ColumnMetaData columnMetaData:columnMetaDataList)	//look at the metadata for each column
			{
Log.trace("looking at column metadata", columnMetaData);	//TODO del
				final String name=columnMetaData.getName();	//get the name of the column
				final Column<?> column=columnMap.get(name);	//see if we have a definition with this name
				if(column!=null)	//if we know about this column
				{
Log.trace("we know about this column", columnMetaData);
					//TODO make sure the definition is the same
					columnMap.remove(name);	//remove the column definition; we've already checked it
				}
				else	//if we don't know about this column
				{
					//TODO delete the column from the underlying table
				}
			}
			for(final Column<?> column:columnMap.values())	//look at the remaining columns TODO important check the order; the mapped order could be anything
			{
Log.trace("we need to add column", column);
				addColumn(column);	//add this column
			}
		}
		else	//if the table does not exist
		{
			create(false);	//create the table; we shouldn't need to drop it first, as it doesn't exist
		}
	}

	/**Retrieves metadata describing the underlying table columns.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	public List<ColumnMetaData> getColumnMetadata() throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final DatabaseMetaData metadata=connection.getMetaData();	//get database metadata
				//use the uppercase form of the table name TODO check to see if this is implementation-specific or part of the JDBC specification
				//SEE http://www.javaworld.com/javaworld/javatips/jw-javatip82.html
				//SEE http://hsqldb.sourceforge.net/doc/src/org/hsqldb/jdbc/jdbcDatabaseMetaData.html#getColumns(java.lang.String,%20java.lang.String,%20java.lang.String,%20java.lang.String)
			final ResultSet resultSet=metadata.getColumns(null, null, getName().toUpperCase(), "%");	//get all columns for this table TODO use a constant for the JDBC wildcard
			try
			{
				final List<ColumnMetaData> columnMetaDataList=new ArrayList<ColumnMetaData>();	//create a new list to hold the column metadata
				while(resultSet.next())	//while ther are more columns
				{
					final String name=resultSet.getString(ColumnMetaData.Columns.COLUMN_NAME.toString());	//get the column name
						//TODO get other column metadata
					final ColumnMetaData columnMetaData=new ColumnMetaData(name);	//create column metadata
					columnMetaDataList.add(columnMetaData);	//add the metadata to our list
				}
				return columnMetaDataList;	//return the list of column metadata
			}
			finally
			{
				resultSet.close();	//always close the result set 
			}
		}
		finally
		{
			connection.close();	//always close the connection
		}
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

	/**Deletes records by primary key columns.
	@param primaryKeyValues The value of the primary keys of the record to delete.
	@exception IllegalArgumentException Thrown if no key values were provided.
	@exception IllegalArgumentException Thrown if there are more values than
		primary key columns.
	@exception SQLException Thrown if there is an error processing the statement.
	@see #getPrimaryKeys()
	*/
	public void deleteByPrimaryKey(final Object... primaryKeyValues) throws SQLException
	{
		if(primaryKeyValues.length==0)	//if no key values were provided
		{
			throw new IllegalArgumentException("No key values were provided");
		}
		delete(createColumnValues(getPrimaryKeys(), primaryKeyValues));  //delete the record based upon the primary keys
	}

	/**Deletes rows from the table for which the given columns contains
		the specified values.
	@param columnValues The columns and values to match.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public void delete(final NameValuePair<Column<?>, ?>... columnValues) throws SQLException
	{
		delete(SQLUtilities.createExpression(Conjunction.AND, createNamesValues(columnValues)));  //delete the records which have the correct value for this columm
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
/*G***del
	public void deleteColumn(final String columnName, final String columnValue) throws SQLException
	{
		delete(columnName+EQUALS+SINGLE_QUOTE+columnValue+SINGLE_QUOTE);  //delete the records which have the correct value for this column
	}
*/

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

	/**Adds a column to the table.
	If the column has a default value, all the rows in the table will be set to the given default value.
	@param column The column to add to the table.
	@exception SQLException Thrown if there is an error accessing the database.
	*/
	protected void addColumn(final Column<?> column) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
					//TODO create a transaction here
				alterTableAddColumn(statement, getName(), column.getName(), getColumnSQLDefinition(column));	//add the column to the table
/*TODO del if not needed; passing a default value seems to automatically update the table when it is added 
				final Object defaultValue=column.getDefaultValue();	//get the column's default value, if there is one
				if(defaultValue!=null)	//if this column has a default value
				{
						//TODO create a method to automatically convert timestamps and the like
					updateTable(statement, getName(), (NameValuePair<String, String>[])new NameValuePair[]{new NameValuePair<String, String>(column.getName(), defaultValue.toString())});	//update the new column with the default value
				}
*/
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
	@param values The values to insert into the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	protected void insert(final Object... values) throws SQLException
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
//TODO del			select("TOP 1 *", null, 0, Integer.MAX_VALUE);  //select the first record from the database TODO use constants, and create a convenience routine for selectExpression methods
			final Connection connection=getDataSource().getConnection();	//get a connection to the database
			try
			{
				final Statement statement=connection.createStatement(); //create a statement
				try
				{
						//execute a query directly; using the table's convenience methods may assume a structure that has not yet been synchronized
					final ResultSet resultSet=statement.executeQuery(SELECT+' '+"TOP 1 *"+' '+FROM+' '+getName()); //select the first record from the database TODO use constants, and create a convenience routine for selectExpression methods
					resultSet.close();	//always close the result set
					return true;	//if we can select records from the table, the table exists
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
		catch(SQLException sqlException)	//if there is any error
		{
			return false;	//assume the table doesn't exist
		}
	}

	/**Selects all the records from the table for which the given columns contains
		the specified values, using this table as the factory to create objects.
	@param columnValues The column-value pairs to match.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final NameValuePair<Column<?>, ?>... columnValues) throws SQLException
	{
		return select(this, columnValues);	//select using this table as a factory
	}

	/**Selects all the records from the table for which the given columns contains
		the specified values.
	@param factory The object factory used to create objects from the result set.
	@param columnValues The column-value pairs to match.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final NameValuePair<Column<?>, ?>... columnValues) throws SQLException
	{
		return select(factory, SQLUtilities.createExpression(Conjunction.AND, createNamesValues(columnValues)));	//select the records which have the correct values for the column
	}

	/**Selects all the records from the table using the given criteria with the
		default ordering, using this table as the factory to create objects.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String expression) throws SQLException
	{
		return select(this, expression);	//select using this table as a factory
	}

	/**Selects all the records from the table using the given criteria with the
		default ordering.
	@param factory The object factory used to create objects from the result set.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String expression) throws SQLException
	{
		return select(factory, expression, 0, Integer.MAX_VALUE);  //return all the rows we can find, starting at the first
	}

	/**Selects all the records from the table using the given criteria, sorting
		on the given column and using this table as the factory to create objects.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String expression, final Column<?>... orderBy) throws SQLException
	{
		return select(this, expression, orderBy);  //use this table as a factory
	}

	/**Selects all the records from the table using the given criteria, sorting
		on the given column.
	@param factory The object factory used to create objects from the result set.
	@param expression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String expression, final Column<?>... orderBy) throws SQLException
	{
		return select(factory, expression, 0, Integer.MAX_VALUE, orderBy);  //return all the rows we can find, starting at the first
	}

	/**Selects all columns of records from the table using the given criteria,
		using this table as the factory to create objects.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String whereExpression, final int startIndex, final int count, Column<?>... orderBy) throws SQLException
	{
		return select(this, whereExpression, startIndex, count, orderBy);	//use this table as a factory
	}

	/**Selects all columns of records from the table using the given criteria.
	@param factory The object factory used to create objects from the result set.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String whereExpression, final int startIndex, final int count, Column<?>... orderBy) throws SQLException
	{
		return select(factory, WILDCARD_STRING, whereExpression, startIndex, count, orderBy);	//select all columns
	}

	/**Selects records from the table using the given criteria, using this
		table as the factory to create objects.
	@param selectExpression The SQL expression that selects the columns.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> select(final String selectExpression, final String whereExpression, final int startIndex, final int count, Column<?>... orderBy) throws SQLException
	{
		return select(this, selectExpression, whereExpression, startIndex, count, orderBy);	//select using this table as a factory
	}

	/**Selects records from the table using the given criteria.
	@param factory The object factory used to create objects from the result set.
	@param selectExpression The SQL expression that selects the columns.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String selectExpression, final String whereExpression, final int startIndex, final int count, Column<?>... orderBy) throws SQLException
	{
		return select(factory, selectExpression, null, whereExpression, startIndex, count, orderBy);	//G***testing
	}

	/**Selects records from the table using the given criteria.
	@param factory The object factory used to create objects from the result set.
	@param selectExpression The SQL expression that selects the columns.
	@param joinExpression The complete SQL expression for joining tables,
		or <code>null</code> if no tables are being joined.
	@param whereExpression The SQL expression that selects the records, or
		<code>null</code> if all records should be returned.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/	//TODO eventually create objects for select, join, where, etc.
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String selectExpression, final String joinExpression, final String whereExpression, final int startIndex, final int count, Column<?>... orderBy) throws SQLException
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
				if(joinExpression!=null)	//if we are joining
				{
				  statementStringBuffer.append(' ').append(joinExpression); //append "<var>joinStatement</var>"
				}
				if(whereExpression!=null && whereExpression.length()>0)  //if a valid expression was given
				{
				  statementStringBuffer.append(' ').append(WHERE).append(' ').append(whereExpression); //append " WHERE <var>whereExpression</var>"
				}
				if(orderBy.length==0)	//if no default ordering was given
				{
					orderBy=getDefaultOrderBy();  //use the default ordering
				}
				if(orderBy.length>0) //if we were given an ordering, or we have a default ordering
				{
				  statementStringBuffer.append(' ').append(ORDER_BY).append(' ').append(createList(orderBy)); //append " ORDER BY orderBy"
				}
//G***del Debug.setDebug(true);
//G***del Log.trace("ready to execute SQL statement: ", statementStringBuffer);	//G***del
				final ResultSet resultSet=statement.executeQuery(statementStringBuffer.toString()); //select the records
				try
				{
					final ArraySubList<F> list=new ArraySubList<F>();	//create a list of results
					list.setStartIndex(startIndex); //show for what index we're returning results
					final int startRow=startIndex+1; //we'll start at the requested row
					final int endRow=count<Integer.MAX_VALUE ? startRow+count : Integer.MAX_VALUE; //we'll end when we get past the requested count (allowing for a requested maximum amount)
				  int row=startRow; //we'll start at the starting row
					boolean onResultSet=resultSet.absolute(startRow); //go to the starting row
//G***del when works					if(!resultSet.isBeforeFirst() && !resultSet.isAfterLast())  //if there are rows, and we haven't gone past all the rows
					while(onResultSet && row<endRow)  //while we're still on the result set and w're not past the ending row
					{
						list.add(factory.retrieve(resultSet)); //retrieve the object from the row and add it to our list
						onResultSet=resultSet.next();	//go to the next row
						++row;  //show that we just went to the next row
					}
					resultSet.last();  //move to the last row
					final int superListSize=resultSet.getRow();  //get the row number, which will be the number of rows in the table
				  list.setSuperListSize(superListSize);  //show how many rows we found
				  return list;	//return the list of objects representing the records we found
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

	/**Selects all columns from records from the table using the given criteria.
	@param factory The object factory used to create objects from the result set.
	@param join The SQL join representation,
		or <code>null</code> if no tables are being joined.
	@param where The SQL records selection, or
		<code>null</code> if all records should be returned.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final Join join, final Where where, final Column<?>... orderBy) throws SQLException
	{
		return select(factory, WILDCARD_STRING, join, where, orderBy);
	}

	/**Selects records from the table using the given criteria.
	@param factory The object factory used to create objects from the result set.
	@param selectExpression The SQL expression that selects the columns.
	@param join The SQL join representation,
		or <code>null</code> if no tables are being joined.
	@param where The SQL records selection, or
		<code>null</code> if all records should be returned.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing matched records.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public <F> SubList<F> select(final ResultSetObjectFactory<F> factory, final String selectExpression, final Join join, final Where where, final Column<?>... orderBy) throws SQLException
	{
		return select(factory, selectExpression, join!=null ? join.toString() : null, where!=null ? where.toString() : null, 0, Integer.MAX_VALUE, orderBy);	//TODO eventually maybe do the serialization here, rather than relying on toString();
	}

	/**Selects all records from the table.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
/**G***del; no longer needed with varargs
	public SubList<T> selectAll() throws SQLException
	{
		return selectAll(null);  //select all records using the default ordering
	}
*/

	/**Selects all records from the table within a given range.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final int startIndex, final int count) throws SQLException
	{
		return selectAll(startIndex, count);  //select all records using the default ordering
	}

	/**Selects all records from the table.
	@param orderBy The columns on which to sort, if any.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final Column<?>... orderBy) throws SQLException
	{
		return select(null, orderBy);  //select all records using the given ordering
	}

	/**Selects all records from the table within a given range.
	@param orderBy The columns on which to sort, if any.
	@param startIndex The index of the first row to retrieve.
	@param count The maximum number of rows to return.
	@return A list of objects representing all records in the table.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public SubList<T> selectAll(final int startIndex, final int count, final Column<?>... orderBy) throws SQLException
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
/*G***del if no longer needed
	public SubList<T> selectColumn(final String columnName, final String columnValue) throws SQLException
	{
		return select(columnName+EQUALS+SINGLE_QUOTE+columnValue+SINGLE_QUOTE);  //select the records which have the correct value for this column
	}
*/

	/**Selects a single record from the table for which the given column contains
		the specified value.
	@param columnName The name of the column to test.
	@param columnValue The column value necessary for a record to be included.
	@return The first object that matches the given criteria, or <code>null</code>
		if no record matches.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
/*G***del if no longer needed
	public T selectColumnRecord(final String columnName, final String columnValue) throws SQLException
	{
		final SubList<T> recordList=selectColumn(columnName, columnValue); //get all the matching records
		return recordList.size()>0 ? recordList.get(0) : null;  //if there are records, return the first record we retrieved; otherwise, return null
	}
*/

	/**Selects a single record by its primary key columns.
	@param primaryKeyValues The value of the primary keys of the record to delete.
	@return The object the primary key of which matches the given value, or
		<code>null</code> if no record matches.
	@exception IllegalArgumentException Thrown if no key values were provided.
	@exception IllegalArgumentException Thrown if there are more values than
		primary key columns.
	@exception SQLException Thrown if there is an error processing the statement.
	@see #getPrimaryKeys()
	*/
	public T selectByPrimaryKey(final Object... primaryKeyValues) throws SQLException
	{
		if(primaryKeyValues.length==0)	//if no key values were provided
		{
			throw new IllegalArgumentException("No key values were provided");
		}
		//TODO update this to look through the columns for the primary key
		final SubList<T> recordList=select(createColumnValues(getPrimaryKeys(), primaryKeyValues));  //select the record based upon the primary key
//TODO probably split out this functionality as it was before		final SubList<T> recordList=selectColumn(columnName, columnValue); //get all the matching records
		return recordList.size()>0 ? recordList.get(0) : null;  //if there are records, return the first record we retrieved; otherwise, return null
	}

	/**Updates a user in the database table.
	@param object The new information for the record.
	@param primaryKeyValues The value of the primary keys of the record to delete.
	@exception IllegalArgumentException Thrown if no key values were provided.
	@exception IllegalArgumentException Thrown if there are more values than
		primary key columns.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
//G***del if not needed	public abstract void update(final T object, final Object... primaryKeyValues) throws SQLException;

	/**Updates records by primary key columns.
	@param updateColumnValues The array of column/value pair arrays to update.
	@param primaryKeyValues The value of the primary keys of the record to delete.
	@exception IllegalArgumentException Thrown if no key values were provided.
	@exception IllegalArgumentException Thrown if there are more values than
		primary key columns.
	@exception SQLException Thrown if there is an error processing the statement.
	@see #getPrimaryKeys()
	*/
	public void updateByPrimaryKey(final NameValuePair<Column<?>, ?>[] updateColumnValues, final Object... primaryKeyValues) throws SQLException
	{
		if(primaryKeyValues.length==0)	//if no key values were provided
		{
			throw new IllegalArgumentException("No key values were provided");
		}
		update(updateColumnValues, createColumnValues(getPrimaryKeys(), primaryKeyValues));  //update the record based upon the primary keys
	}

	/**Inserts values into the table.
	@param updateColumnValues The array of column/value pair arrays to update.
	@param whereColumnValues The column names and values to match.
	@exception SQLException Thrown if there is an error processing the statement.
	*/	//TODO we probably need a way to make sure at least one column is passed; otherwise, update would probably update all records
	protected void update(final NameValuePair<Column<?>, ?>[] updateColumnValues, final NameValuePair<Column<?>, ?>... whereColumnValues) throws SQLException
	{
		final Connection connection=getDataSource().getConnection();	//get a connection to the database
		try
		{
			final Statement statement=connection.createStatement(); //create a statement
			try
			{
				  //update the values for the row with the primary key value
				SQLUtilities.updateTable(statement, getName(), createNamesValues(updateColumnValues), SQLUtilities.createExpression(Conjunction.AND, createNamesValues(whereColumnValues)));
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

	/**Creates an array of column-value pairs.
	Only columns for which values are provided will be included.
	@param columns The columns to include.
	@param values The value to match with columns.
	@return An array containing pairs of columns and values.
	@exception IllegalArgumentException Thrown if there are more values than
		columns.
	*/
	public static NameValuePair<Column<?>, ?>[] createColumnValues(final Column<?>[] columns, final Object[] values)
	{
		if(values.length>columns.length)	//if there are more values than columns
		{
			throw new IllegalArgumentException("There are "+values.length+" values but only "+columns.length+" columns.");
		}
		final NameValuePair<Column<?>, ?>[] columnValues=new NameValuePair[values.length];	//create a new array to hold the column-value pairs
		for(int i=values.length-1; i>=0; --i)	//look at each value
		{
				//create a name-value pair with the column and the value
			columnValues[i]=new NameValuePair<Column<?>, Object>(columns[i], values[i]);
		}
		return columnValues;	//return the array of columns and values		
	}

	/**Creates an array of name-value pairs containing the names of the given
		columns and their values.
	@param columnValues The columns and related values.
	@return An array containing pairs of column names and values.
	*/
	public static NameValuePair<String, String>[] createNamesValues(final NameValuePair<Column<?>, ?>[] columnValues)
	{
		final NameValuePair<String, String>[] namesValues=new NameValuePair[columnValues.length];	//create a new array
		for(int i=columnValues.length-1; i>=0; --i)	//look at each column-value pair
		{
			final Object value=columnValues[i].getValue();	//get the column value
				//create a name-value pair with the column name and the value, or null if there is no value
			namesValues[i]=new NameValuePair<String, String>(columnValues[i].getName().getName(), value!=null ? value.toString() : null);
		}
		return namesValues;	//return the array of names and values		
	}

	/**Creates a string representing a list of columns in SQL.
	@param columns The columns to be placed in a list.
	*/
	public static String createList(final Column<?>... columns)
	{
		final String[] columnNames=new String[columns.length];	//create an array of items
		for(int i=columns.length-1; i>=0; --i)	//look at each column
		{
			columnNames[i]=columns[i].getName();	//store this column name
		}
		return SQLUtilities.createList(columnNames);	//create an SQL list from the column names
	}

}
