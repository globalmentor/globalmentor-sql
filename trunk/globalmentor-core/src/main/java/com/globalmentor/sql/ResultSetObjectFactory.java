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