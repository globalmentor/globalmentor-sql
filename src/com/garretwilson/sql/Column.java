package com.garretwilson.sql;

import com.garretwilson.util.NameValuePair;

/**A class encapsulating the definition of a database column.
@author Garret Wilson
*/
public class Column extends NameValuePair<String, String>
{
	/**@return The type of the column.
	@see NameValuePair#getValue()
	*/
	public String getType() {return getValue();}
	
	/**<code>true</code> if this column is a primary key.*/
	private final boolean primaryKey;

		/**@return <code>true</code> if this column is a primary key.*/
		public boolean isPrimaryKey() {return primaryKey;}

	/**Name and type constructor for a non-primary key column.
	@param name The name of the column.
	@param type The type of the column.
	*/
	public Column(final String name, final String type)
	{
		this(name, type, false);	//construct a column that is not a primary key
	}

	/**Name, type, and primary key constructor.
	@param name The name of the column.
	@param type The type of the column.
	@param primaryKey <code>true</code> if this column is a primary key.
	*/
	public Column(final String name, final String type, final boolean primaryKey)
	{
		super(name, type);	//construct the base class with the name and the type as the value
		this.primaryKey=primaryKey;	//save the primary key indication
	}
	
}
