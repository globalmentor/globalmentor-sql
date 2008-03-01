package com.garretwilson.sql;

import com.globalmentor.util.DefaultComparableNamed;

/**Metadata regarding a column as retrieved from JDBC.
@author Garret Wilson
*/
public class ColumnMetaData extends DefaultComparableNamed<String>
{
	/**The names of the columns of the result set describing each column.*/
	public enum Columns
	{
		/**Table catalog (String); may be <code>null</code>.*/
		TABLE_CAT,
		/**Table schema (String); may be <code>null</code>.*/
		TABLE_SCHEM,
		/**Table name (String).*/
		TABLE_NAME,
		/**Column name (String).*/
		COLUMN_NAME,
		/**SQL type from <code>java.sql.Types</code> (int).*/
		DATA_TYPE,
		/**Data source dependent type name (String); for a UDT the type name is fully qualified.*/
		TYPE_NAME,
		/**Column size (int). For char or date types this is the maximum number of characters; for numeric or decimal types this is precision.*/
		COLUMN_SIZE,
		/**Buffer length (unused).*/
		BUFFER_LENGTH,
		/**The number of fractional digits (int).*/
		DECIMAL_DIGITS,
		/**Radix (int); typically either 10 or 2.*/
		NUM_PREC_RADIX,
		/**Whether NULL is allowed (int).
		<dl>
			<dt><code>columnNoNulls</code></dt> <dd>might not allow NULL values</dd>
			<dt><code>columnNullable</code></dt> <dd>definitely allows NULL values</dd>
			<dt><code>columnNullableUnknown</code></dt> <dd>nullability unknown</dd> 
		</dl>
		*/
		NULLABLE,
		/**Comment describing column (String); may be <code>null</code>.*/
		REMARKS,
		/**Default value (String); may be <code>null</code>.*/
		COLUMN_DEF,
		/**SQL data type (int); unused.*/
		SQL_DATA_TYPE,
		/**SQL date-time sub (int); unused.*/
		SQL_DATETIME_SUB,
		/**For char types, the maximum number of bytes in the column (int).*/
		CHAR_OCTET_LENGTH,
		/**Index of column in table (starting at 1) (int).*/
		ORDINAL_POSITION,
		/**Whether the column is nullable (String); "NO" means column definitely does not allow NULL values; "YES" means the column might allow NULL values. An empty string means nobody knows..*/
		IS_NULLABLE,
		/**Catalog of table that is the scope of a reference attribute (String); <code>null</code> if DATA_TYPE isn't REF.*/
		SCOPE_CATLOG,
		/**Schema of table that is the scope of a reference attribute (String); <code>null</code> if the DATA_TYPE isn't REF.*/
		SCOPE_SCHEMA,
		/**Table name that this the scope of a reference attribure (String); <code>null</code> if the DATA_TYPE isn't REF.*/
		SCOPE_TABLE,
		/**Source type of a distinct type or user-generated Ref type, SQL type from <code>java.sql.Types</code> (short); <code>null</code> if DATA_TYPE isn't DISTINCT or user-generated REF.*/
		SOURCE_DATA_TYPE
	}

	/**Constructor specifying the name.
	@param name The column name.
	*/
	public ColumnMetaData(final String name)
	{
		super(name);	//construct the parent class
	}

}
