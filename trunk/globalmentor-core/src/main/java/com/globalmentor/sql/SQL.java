/*
 * Copyright © 1996-2009 GlobalMentor, Inc. <http://www.globalmentor.com/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.globalmentor.sql;

import java.sql.*;

import com.globalmentor.java.Strings;
import com.globalmentor.model.NameValuePair;

/**
 * Class that knows how to manipulate SQL statements.
 * @author Garret Wilson
 */
public class SQL {

	/** Conjunctions used in SQL expressions. */
	public enum Conjunction {
		AND, OR
	};

	/** The character that quotes strings in SQL values. */
	public final static char SINGLE_QUOTE = '\'';
	/** The escaped form of a single quote. */
	public final static String ESCAPED_SINGLE_QUOTE = "''";
	/** The SQL equals character. */
	public final static char EQUALS = '=';
	/** The SQL list separator character. */
	public final static char LIST_SEPARATOR = ',';
	/** The SQL table.column separator character. */
	public final static char TABLE_COLUMN_SEPARATOR = '.';

	/** The SQL wildcard ('*') character. */
	public final static char WILDCARD_CHAR = '*';

	/** The SQL ADD command. */
	public final static String ADD = "ADD";
	/** The SQL ALTER command. */
	public final static String ALTER = "ALTER";
	/** The SQL AND keyword. */
	//TODO del when works	public final static String AND="AND";
	/** The SQL BY keyword. */
	public final static String BY = "BY";
	/** The SQL CREATE command. */
	public final static String CREATE = "CREATE";
	/** The SQL DEFAULT keyword. */
	public final static String DEFAULT = "DEFAULT";
	/** The SQL DELETE command. */
	public final static String DELETE = "DELETE";
	/** The SQL DROP command. */
	public final static String DROP = "DROP";
	/** The SQL EXISTS keyword. */
	public final static String EXISTS = "EXISTS";
	/** The SQL FROM keyword. */
	public final static String FROM = "FROM";
	/** The SQL IF keyword. */
	public final static String IF = "IF";
	/** The SQL KEY keyword. */
	public final static String KEY = "KEY";
	/** The SQL INSERT command. */
	public final static String INSERT = "INSERT";
	/** The SQL INTO keyword. */
	public final static String INTO = "INTO";
	/** The SQL JOIN command. */
	public final static String JOIN = "JOIN";
	/** The SQL NULL keyword. */
	public final static String NULL = "NULL";
	/** The SQL ON keyword. */
	public final static String ON = "ON";
	/** The SQL ORDER keyword. */
	public final static String ORDER = "ORDER";
	/** The SQL ORDER BY phrase. */
	public final static String ORDER_BY = ORDER + ' ' + BY;
	/** The SQL PRIMARY keyword. */
	public final static String PRIMARY = "PRIMARY";
	/** The SQL PRIMARY KEY phrase. */
	public final static String PRIMARY_KEY = PRIMARY + ' ' + KEY;
	/** The SQL SELECT command. */
	public final static String SELECT = "SELECT";
	/** The SQL SET command. */
	public final static String SET = "SET";
	/** The SQL TABLE keyword. */
	public final static String TABLE = "TABLE";
	/** The SQL UPDATE command. */
	public final static String UPDATE = "UPDATE";
	/** The SQL VALUES keyword. */
	public final static String VALUES = "VALUES";
	/** The SQL WHERE keyword. */
	public final static String WHERE = "WHERE";

	/**
	 * Creates a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to be created.
	 * @param definition The definition string of the table, appropriate for SQL CREATE TABLE XXX (definition).
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void createTable(final Statement statement, final String name, final String definition) throws SQLException {
		statement.executeUpdate(CREATE + " " + TABLE + " " + name + " (" + definition + ")"); //execute the SQL create statement
	}

	/**
	 * Deletes one or more rows from a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table from which rows will be removed.
	 * @param expression The expression describing the rows to be removed.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void delete(final Statement statement, final String name, final String expression) throws SQLException {
		statement.executeUpdate(DELETE + " " + FROM + " " + name + " " + WHERE + " " + expression); //execute the SQL create statement
	}

	/**
	 * Drops (removes) a table using SQL commands if it exists.
	 * @param statement The SQL statement.
	 * @param name The name of the table to be dropped.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void dropTable(final Statement statement, final String name) throws SQLException {
		dropTable(statement, name, true); //drop the table, checking for existence
	}

	/**
	 * Drops (removes) a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to be dropped.
	 * @param ifExists <code>true</code> if the "IF EXISTS" condition should be added.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void dropTable(final Statement statement, final String name, final boolean ifExists) throws SQLException {
		final StringBuffer statementStringBuffer = new StringBuffer(); //create a string buffer in which to construct the statement
		statementStringBuffer.append(DROP).append(' ').append(TABLE).append(' ').append(name); //append "DROP TABLE name"
		if(ifExists) //if we should add the existence check
			statementStringBuffer.append(' ').append(IF).append(' ').append(EXISTS); //append " IF EXISTS"
		statement.executeUpdate(statementStringBuffer.toString()); //execute the SQL drop statement
	}

	/**
	 * Adds a column to a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param tableName The name of the table to which a column should be added.
	 * @param columnName The name of the column to add.
	 * @param columnDefinition The definition string of the column.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void alterTableAddColumn(final Statement statement, final String tableName, final String columnName, final String columnDefinition)
			throws SQLException {
		statement.executeUpdate(ALTER + ' ' + TABLE + ' ' + tableName + ' ' + ADD + ' ' + columnName + ' ' + columnDefinition); //execute the SQL alter table statement
	}

	/**
	 * Inserts values into a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table into which data will be inserted.
	 * @param valueString The string of comma-separated values to go inside SQL INSERT INTO XXX VALUES (values).
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void insertValues(final Statement statement, final String name, final String valueString) throws SQLException {
		statement.executeUpdate(INSERT + " " + INTO + " " + name + " VALUES (" + valueString + ")"); //execute the SQL create statement
	}

	/**
	 * Inserts values into a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table into which data will be inserted.
	 * @param values The values to go inside SQL INSERT INTO XXX VALUES (values).
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void insertValues(final Statement statement, final String name, final Object... values) throws SQLException { //TODO fix to work with integer values
		final StringBuffer valueStringBuffer = new StringBuffer(); //we'll store the values in this string
		for(int i = 0; i < values.length; ++i) { //look at each of the values
			final Object value = values[i]; //get this value
			if(value != null) { //if we have a valid value	TODO refactor with a new createSQLValue() method that takes care of all this
				valueStringBuffer.append(SINGLE_QUOTE);
				valueStringBuffer.append(createSQLValue(value.toString())); //add the string representation of this object to our value string
				valueStringBuffer.append(SINGLE_QUOTE);
			} else { //if we don't have a valid value
				valueStringBuffer.append(NULL); //add an SQL NULL value
			}
			if(i < values.length - 1) { //if we have more values to go
				valueStringBuffer.append(LIST_SEPARATOR).append(' '); //separate the values
			}
		}
		insertValues(statement, name, valueStringBuffer.toString()); //perform the insert with the value stream we constructed
	}

	/**
	 * Updates all values in a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to update.
	 * @param valueString The string of comma-separated values to go inside SQL UPDATE XXX SET valueString.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void updateTable(final Statement statement, final String name, final String valueString) throws SQLException {
		updateTable(statement, name, valueString, null); //update the table with no predicate
	}

	/**
	 * Updates values in a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to update.
	 * @param valueString The string of comma-separated values to go inside SQL UPDATE XXX SET valueString.
	 * @param predicate The condition on which to make the changes, or <code>null</code> if all rows should be updated.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void updateTable(final Statement statement, final String name, final String valueString, final String predicate) throws SQLException {
		final StringBuffer statementStringBuffer = new StringBuffer(); //we'll store the statement in this string
		statementStringBuffer.append(UPDATE).append(' ').append(name); //"UPDATE name"
		statementStringBuffer.append(' ').append(SET).append(' ').append(valueString); //" SET valueString"
		if(predicate != null) //if there is a predicate
			statementStringBuffer.append(' ').append(WHERE).append(' ').append(predicate); //add the predicate, in the form " WHERE predicate"
		statement.executeUpdate(statementStringBuffer.toString()); //execute the SQL update statemen
	}

	/**
	 * Updates all values in a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to update.
	 * @param namesValues The array of name/value pair arrays to update.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void updateTable(final Statement statement, final String name, final NameValuePair<String, String>[] namesValues) throws SQLException {
		updateTable(statement, name, namesValues, null); //update the table with no predicate
	}

	/**
	 * Updates values in a table using SQL commands.
	 * @param statement The SQL statement.
	 * @param name The name of the table to update.
	 * @param namesValues The array of column/value pair arrays to update.
	 * @param predicate The condition on which to make the changes, or <code>null</code> if all rows should be updated.
	 * @throws SQLException Thrown if there is an error processing the statement.
	 */
	public static void updateTable(final Statement statement, final String name, final NameValuePair<String, String>[] namesValues, final String predicate)
			throws SQLException {
		final StringBuffer valueStringBuffer = new StringBuffer(); //we'll construct the value string from the names and values
		for(int i = 0; i < namesValues.length; ++i) { //look at each of the name/value pair arrays
			valueStringBuffer.append(namesValues[i].getName()); //append the name
			valueStringBuffer.append(EQUALS); //append '='
			final Object value = namesValues[i].getValue(); //TODO refactor
			if(value != null) { //if we have a valid value	TODO refactor with a new createSQLValue() method that takes care of all this
				valueStringBuffer.append(SINGLE_QUOTE);
				valueStringBuffer.append(createSQLValue(value.toString())); //append the value
				valueStringBuffer.append(SINGLE_QUOTE);
			} else { //if we don't have a valid value
				valueStringBuffer.append(NULL); //add an SQL NULL value
			}
			if(i < namesValues.length - 1) { //if we have more names and values to go
				valueStringBuffer.append(LIST_SEPARATOR).append(' '); //separate the values
			}
		}
		updateTable(statement, name, valueStringBuffer.toString(), predicate); //update the table with our constructed value string
	}

	/*TODO fix Creates an expression in the form
	public static createEqualsExpression(final String columnName, final String Expression)
	{

	}
	*/

	/**
	 * Creates a value that is compatible with SQL by replacing all single quotes (') with double single quotes ('').
	 * @param value The value to encode.
	 * @return The value encoded for
	 */
	public static String createSQLValue(final String value) {
		return Strings.replace(value, SINGLE_QUOTE, ESCAPED_SINGLE_QUOTE); //replace all single quotes with double single quotes
	}

	/**
	 * Creates an expression matching names and values, combined by the given conjunction. (e.g. "COL1="val1" AND COL2="val2")
	 * @param conjunction The conjunction to use when combining the columns in the expression.
	 * @param columns The names and values of the columns to match.
	 * @return The string form of the resulting expression.
	 */
	public static String createExpression(final Conjunction conjunction, NameValuePair<String, String>... columns) { //TODO replace conjunction with enum
		final StringBuilder expression = new StringBuilder();
		for(int i = 0; i < columns.length; ++i) { //look at each column matching information
			final NameValuePair<String, String> column = columns[i]; //get this column
			expression.append(column.getName()); //name
			expression.append(EQUALS); //EQUALS
			expression.append(SINGLE_QUOTE); //'
			expression.append(createSQLValue(column.getValue())); //value
			expression.append(SINGLE_QUOTE); //'
			if(i < columns.length - 1) { //if this is not the last column to match
				expression.append(' ').append(conjunction).append(' '); // conjunction 
			}
		}
		return expression.toString(); //return the string form of the expression we built 
	}

	/**
	 * Creates a string representing an SQL list.
	 * @param items The items contained in the list, such as column names.
	 */
	public static String createList(final String... items) {
		final StringBuilder list = new StringBuilder();
		for(int i = 0; i < items.length; ++i) { //look at each item
			list.append(items[i]); //item
			if(i < items.length - 1) { //if this is not the last item in the list
				list.append(LIST_SEPARATOR).append(' '); // ,
			}
		}
		return list.toString(); //return the string form of the list we built
	}

}
