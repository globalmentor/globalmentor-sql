package com.garretwilson.sql;

import com.garretwilson.util.NameValuePair;

import static com.garretwilson.sql.SQLConstants.*;

/**A class encapsulating the definition of a database column.
@author Garret Wilson
*/
public class Column extends NameValuePair<String, String>
{

	/**The table with which this column has been associated.*/
//G***del	private Table table=null;

		/**Associates a table with a column.
		@param table The table with which this column is being associated.
		*/
//G***del		void setTable(final Table table) {this.table=table;}

		/**@return The table with which this column has been associated, or
			<code>null</code> if this column has not been associated with a table.
		*/
//G***del		public Table getTable() {return table;}

	/**The name of the table with which this column is associated.*/
	private final String tableName;

		/**@return The name of the table with which this column is associated.*/
		public String getTableName() {return tableName;}

	/**@return The type of the column.
	@see NameValuePair#getValue()
	*/
	public String getType() {return getValue();}
	
	/**<code>true</code> if this column is a primary key.*/
	private final boolean primaryKey;

		/**@return <code>true</code> if this column is a primary key.*/
		public boolean isPrimaryKey() {return primaryKey;}

	/**Name and type constructor for a non-primary key column.
	@param tableName The name of the table with which this column is associated.
	@param name The name of the column.
	@param type The type of the column.
	*/
	public Column(final String tableName, final String name, final String type)
	{
		this(tableName, name, type, false);	//construct a column that is not a primary key
	}

	/**Name, type, and primary key constructor.
	@param tableName The name of the table with which this column is associated.
	@param name The name of the column.
	@param type The type of the column.
	@param primaryKey <code>true</code> if this column is a primary key.
	*/
	public Column(final String tableName, final String name, final String type, final boolean primaryKey)
	{
		super(name, type);	//construct the base class with the name and the type as the value
		this.tableName=tableName;		//save the table name
		this.primaryKey=primaryKey;	//save the primary key indication
	}

	/**@return A string representing this column appropriate for an SQL statement.*/
	public String toString()
	{
		final StringBuilder stringBuilder=new StringBuilder();
//G***del if not needed		if(getTable()!=null)	//if this column is associated with a table
		{
			stringBuilder.append(tableName).append(TABLE_COLUMN_SEPARATOR);	//tableName.
		}
		stringBuilder.append(getName());	//columnName
		return stringBuilder.toString();	//return the representation of the column
	}
	
}
