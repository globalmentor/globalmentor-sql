package com.garretwilson.sql;

import java.util.*;
import com.garretwilson.util.*;

import static com.garretwilson.sql.SQLConstants.*;

/**Encapsulates an SQL expression for JOIN.
@author Garret Wilson
*/
public class Join
{

	/**The column-column pairs to join.*/
	private final NameValuePair<Column, Column>[] joins;

		/**@return The column-column pairs to match.*/
		protected NameValuePair<Column, Column>[] getJoins() {return joins;}

	/**Creates an expression joining columns.
	@param joins The column-column pairs to join, where the name column belongs
		to the joining table and the value column belongs to the table being
	 joined.
	*/	
	public Join(final NameValuePair<Column, Column>... joins)
	{
		this.joins=joins;	//save the column-column pairs
	}

	/**Creates an SQL string version of this expression.*/
	public String toString()
	{
		final StringBuilder expression=new StringBuilder();
		for(final NameValuePair<Column, Column> join:getJoins())	//look at each join
		{
			expression.append(JOIN).append(' ').append(join.getValue().getTableName());	//JOIN table2
			expression.append(' ').append(ON).append(' ');	// ON
			expression.append(join.getName()).append(EQUALS).append(join.getValue());	//table1.column1=table2.column2
		}
		return expression.toString();	//return our expression
	}
}
