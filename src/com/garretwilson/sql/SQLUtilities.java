package com.garretwilson.sql;

import java.sql.*;
import com.garretwilson.lang.StringUtilities;

/**Class that knows how to manipulate SQL statements.
@author Garret Wilson
@see SQLConstants
*/
public class SQLUtilities implements SQLConstants
{

	/**Creates a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to be created.
	@param definition The definition string of the table, appropriate for
		SQL CREATE TABLE XXX (definition).
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void createTable(final Statement statement, final String name, final String definition) throws SQLException
	{
		statement.executeUpdate(CREATE+" "+TABLE+" "+name+" ("+definition+")");	//execute the SQL create statement
	}

	/**Deletes one or more rows from a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table from which rows will be removed.
	@param expression The expression describing the rows to be removed.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void delete(final Statement statement, final String name, final String expression) throws SQLException
	{
		statement.executeUpdate(DELETE+" "+FROM+" "+name+" "+WHERE+" "+expression);	//execute the SQL create statement
	}

	/**Drops (removes) a table using SQL commands if it exists.
	@param statement The SQL statement.
	@param name The name of the table to be dropped.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void dropTable(final Statement statement, final String name) throws SQLException
	{
		dropTable(statement, name, true); //drop the table, checking for existence
	}

	/**Drops (removes) a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to be dropped.
	@param ifExists <code>true</code> if the "IF EXISTS" condition should be added.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void dropTable(final Statement statement, final String name, final boolean ifExists) throws SQLException
	{
		final StringBuffer statementStringBuffer=new StringBuffer();  //create a string buffer in which to construct the statement
		statementStringBuffer.append(DROP).append(' ').append(TABLE).append(' ').append(name); //append "DROP TABLE name"
		if(ifExists)  //if we should add the existence check
		  statementStringBuffer.append(' ').append(IF).append(' ').append(EXISTS);  //append " IF EXISTS"
		statement.executeUpdate(statementStringBuffer.toString());	//execute the SQL drop statement
	}

	/**Inserts values into a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table into which data will be inserted.
	@param valueString The string of comma-separated values to go inside SQL INSERT INTO XXX VALUES (values).
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void insertValues(final Statement statement, final String name, final String valueString) throws SQLException
	{
		statement.executeUpdate(INSERT+" "+INTO+" "+name+" VALUES ("+valueString+")");	//execute the SQL create statement
	}

	/**Inserts values into a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table into which data will be inserted.
	@param valueArray The array of values to go inside SQL INSERT INTO XXX VALUES (values).
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void insertValues(final Statement statement, final String name, final Object[] valueArray) throws SQLException  //G***fix to work with integer values
	{
		final StringBuffer valueStringBuffer=new StringBuffer();  //we'll store the values in this string
		for(int i=0; i<valueArray.length; ++i)	//look at each of the values
		{
			final Object value=valueArray[i]; //get this value
			if(value!=null) //if we have a valid value
			{
				valueStringBuffer.append(SINGLE_QUOTE);
			  valueStringBuffer.append(createSQLValue(value.toString())); //add the string representation of this object to our value string
				valueStringBuffer.append(SINGLE_QUOTE);
			}
			else  //if we don't have a valid value
			{
			  valueStringBuffer.append(NULL); //add an SQL NULL value
			}
			if(i<valueArray.length-1) //if we have more values to go
			{
				valueStringBuffer.append(LIST_SEPARATOR).append(' ');  //separate the values
			}
		}
		insertValues(statement, name, valueStringBuffer.toString());	//perform the insert with the value stream we constructed
	}

	/**Updates all values in a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to update.
	@param valueString The string of comma-separated values to go inside SQL UPDATE XXX SET valueString.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void updateTable(final Statement statement, final String name, final String valueString) throws SQLException
	{
		updateTable(statement, name, valueString, null); //update the table with no predicate
	}

	/**Updates values in a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to update.
	@param valueString The string of comma-separated values to go inside SQL UPDATE XXX SET valueString.
	@param predicate The condition on which to make the changes, or
		<code>null</code> if all rows should be updated.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void updateTable(final Statement statement, final String name, final String valueString, final String predicate) throws SQLException
	{
		final StringBuffer statementStringBuffer=new StringBuffer();  //we'll store the statement in this string
		statementStringBuffer.append(UPDATE).append(' ').append(name);  //"UPDATE name"
		statementStringBuffer.append(' ').append(SET).append(' ').append(valueString); //" SET valueString"
		if(predicate!=null) //if there is a predicate
			statementStringBuffer.append(' ').append(WHERE).append(' ').append(predicate);  //add the predicate, in the form " WHERE predicate"
		statement.executeUpdate(statementStringBuffer.toString());	//execute the SQL update statemen
	}

	/**Updates all values in a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to update.
	@param namesValues The array of name/value pair arrays to update.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void updateTable(final Statement statement, final String name, final String[][] namesValues) throws SQLException
	{
		updateTable(statement, name, namesValues, null); //update the table with no predicate
	}

	/**Updates values in a table using SQL commands.
	@param statement The SQL statement.
	@param name The name of the table to update.
	@param namesValues The array of name/value pair arrays to update.
	@param predicate The condition on which to make the changes, or
		<code>null</code> if all rows should be updated.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public static void updateTable(final Statement statement, final String name, final Object[][] namesValues, final String predicate) throws SQLException
	{
		final StringBuffer valueStringBuffer=new StringBuffer(); //we'll construct the value string from the names and values
		for(int i=0; i<namesValues.length; ++i) //look at each of the name/value pair arrays
		{
			valueStringBuffer.append(namesValues[i][0].toString());  //append the name
			valueStringBuffer.append(EQUALS); //append '='
			valueStringBuffer.append(SINGLE_QUOTE);
			valueStringBuffer.append(createSQLValue(namesValues[i][1].toString())); //append the value
			valueStringBuffer.append(SINGLE_QUOTE);
			if(i<namesValues.length-1) //if we have more names and values to go
			{
				valueStringBuffer.append(LIST_SEPARATOR).append(' ');  //separate the values
			}
		}
		updateTable(statement, name, valueStringBuffer.toString(), predicate);  //update the table with our constructed value string
	}

	/*G***fix Creates an expression in the form
	public static createEqualsExpression(final String columnName, final String Expression)
	{

	}
*/

	/**Creates a value that is compatible with SQL by replacing all single quotes
		(') with double single quotes ('').
	@param value The value to encode.
	@return The value encoded for SQL.
	*/
	public static String createSQLValue(final String value)
	{
		return StringUtilities.replace(value, SINGLE_QUOTE, ESCAPED_SINGLE_QUOTE);  //replace all single quotes with double single quotes
	}

}