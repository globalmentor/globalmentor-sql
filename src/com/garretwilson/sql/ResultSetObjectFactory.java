package com.garretwilson.sql;

import java.sql.*;

public interface ResultSetObjectFactory<T>
{
	/**Creates a new object and retrieves its contents from the current row of
		the given result set.
	@param resultSet The result set that contains the object information.
	@return A new object with information from the current row in the result set.
	@exception SQLException Thrown if there is an error processing the statement.
	*/
	public T retrieve(final ResultSet resultSet) throws SQLException;
}