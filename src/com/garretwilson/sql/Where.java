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

package com.garretwilson.sql;

import com.globalmentor.model.NameValuePair;

import static com.garretwilson.sql.SQL.*;

/**Encapsulates an SQL expression for WHERE.
@author Garret Wilson
*/
public class Where
{

	/**The conjunction (AND or OR) for matching the columns.*/
	private final SQL.Conjunction conjunction;

		/**The conjunction (AND or OR) for matching the columns.*/
		protected SQL.Conjunction getConjunction() {return conjunction;}

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
		this(SQL.Conjunction.AND, columnValues);	//default requiring all columns and values to match
	}

	/**Creates an expression matching columns and values.
	@param conjunction The conjunction (AND or OR) for requiring matches.
	@param columnValues The column-value pairs to match.
	*/	
	public Where(final SQL.Conjunction conjunction, final NameValuePair<Column<?>, ?>... columnValues)
	{
		this.conjunction=conjunction;	//save the conjunction
		this.columnValues=columnValues;	//save the column-value pairs
	}

	/**Creates an SQL string version of this expression.*/
	public String toString()
	{
		return createExpression(getConjunction(), Table.createNamesValues(getColumnValues()));	//create the expression using our conjunction with the columns and values
	}
}