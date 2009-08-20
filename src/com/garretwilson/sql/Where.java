package com.garretwilson.sql;

import com.globalmentor.model.NameValuePair;
import com.globalmentor.util.*;

import static com.garretwilson.sql.SQLConstants.*;

/**Encapsulates an SQL expression for WHERE.
@author Garret Wilson
*/
public class Where
{

	/**The conjunction (AND or OR) for matching the columns.*/
	private final Conjunction conjunction;

		/**The conjunction (AND or OR) for matching the columns.*/
		protected Conjunction getConjunction() {return conjunction;}

	/**The column-value pairs to match.*/
	private final NameValuePair<Column<?>, ?>[] columnValues;

		/**@return The column-value pairs to match.*/
		protected NameValuePair<Column<?>, ?>[] getColumnValues() {return columnValues;}

	/**Creates an expression matching columns and values.
	Requires all columns and values to match.
	@param columnValues The column-value pairs to match.
	*/	
	public Where(final NameValuePair<Column<?>, ?>... columnValues)
	{
		this(Conjunction.AND, columnValues);	//default requiring all columns and values to match
	}

	/**Creates an expression matching columns and values.
	@param conjunction The conjunction (AND or OR) for requiring matches.
	@param columnValues The column-value pairs to match.
	*/	
	public Where(final Conjunction conjunction, final NameValuePair<Column<?>, ?>... columnValues)
	{
		this.conjunction=conjunction;	//save the conjunction
		this.columnValues=columnValues;	//save the column-value pairs
	}

	/**Creates an SQL string version of this expression.*/
	public String toString()
	{
		return SQLUtilities.createExpression(getConjunction(), Table.createNamesValues(getColumnValues()));	//create the expression using our conjunction with the columns and values
	}
}