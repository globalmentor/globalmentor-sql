package com.garretwilson.sql;

import com.garretwilson.util.*;

import static com.garretwilson.sql.SQLConstants.*;

/**Encapsulates an SQL expression for WHERE.
@author Garret Wilson
*/
public class Where
{

	/**The column-value pairs to match.*/
	private final NameValuePair<Column, ?>[] columnValues;

		/**@return The column-value pairs to match.*/
		protected NameValuePair<Column, ?>[] getColumnValues() {return columnValues;}

	/**Creates an expression matching columns and values.
	@param columnValues The column-value pairs to match.
	*/	
	public Where(final NameValuePair<Column, ?>... columnValues)
	{
		this.columnValues=columnValues;	//save the column-value pairs
	}

	/**Creates an SQL string version of this expression.*/
	public String toString()
	{
		return SQLUtilities.createExpression(Conjunction.AND, Table.createNamesValues(getColumnValues()));	//create the union of the columns and values
	}
}