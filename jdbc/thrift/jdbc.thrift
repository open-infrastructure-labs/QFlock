
namespace * com.github.qflock.jdbc.api

exception InvalidOperation {
  1: i32 errorCode,
  2: string message
}

struct QFConnection
{
  1: i32 id
}

struct QFStatement
{
  1: i32 id,
  2: string sql
  3: i32 id_connection
}
struct QFPreparedStatement
{
  1: i32 id,
  2: string sql
  3: i32 id_connection
}

struct QFProperty
{
  1: string key,
  2: string value
}

union RawVal {
  1: i64 bigint_val,
  2: i32 integer_val,
  3: i16 smallint_val,
  4: double double_val,
  5: bool bool_val,
  6: string string_val
}

struct QFValue
{
  1: bool isnull,
  2: RawVal val
}


struct QFRow
{
  1: list<QFValue> values
}

struct QFResultSetMetaDataPart
{
  1:  string catalogName,
  2:  string columnClassName,
  3:  i32 columnDisplaySize,
  4:  string columnLabel,
  5:  string columnName,
  6:  i32 columnType,
  7:  string columnTypeName,
  8:  i32 precision,
  9:  i32 scale,
  10:  string schemaName,
  11:  string tableName,
  12:  bool autoIncrement,
  13:  bool caseSensitive,
  14:  bool currency,
  15:  bool definitelyWritable,
  16:  i32 nullable,
  17:  bool readOnly,
  18:  bool searchable,
  19:  bool signed,
  20:  bool writable
}

struct QFResultSetMetaData
{
  1: list<QFResultSetMetaDataPart> parts
}

struct QFResultSet
{
  1: i32 id,
  2: QFResultSetMetaData metadata,
  3: i32 numRows,
  4: list<binary> binaryRows,
  5: list<i32> columnTypeBytes,
  6: list<i32> columnBytes,
  7: list<binary> compressedRows,
  8: list<i32> compressedColumnBytes,
}

struct QFStaticMetaData
{
  1: i32 databaseMajorVersion,
  2: i32 databaseMinorVersion,
  3: string databaseProductName,
  4: string databaseProductVersion,
  5: i32 defaultTransactionIsolation,
  6: string identifierQuoteString,
  7: bool supportsCatalogsInTableDefinitions,
  8: bool supportsSavepoints,
  9: bool supportsSchemasInDataManipulation,
  10: bool supportsSchemasInTableDefinitions
}

struct QFSQLWarning {
  1: string reason
  2: string state
  3: i32 vendorCode
}

struct statement_getWarnings_return {
  1: list<QFSQLWarning> warnings
}

struct preparedStatement_getWarnings_return {
  1: list<QFSQLWarning> warnings
}

exception QFSQLException {
  1: string reason
  2: string sqlState
  3: i32 vendorCode
}

service QflockJdbcService {

   QFConnection createConnection(1:string url, 2:map<string,string> properties),

   QFStatement createStatement(1:QFConnection connection),
   QFPreparedStatement createPreparedStatement(1:QFConnection connection),

   QFStaticMetaData connection_getstaticmetadata(1:QFConnection connection),
   bool connection_isvalid(1:QFConnection connection, 2:i32 timeout),

   void connection_setAutoCommit(1:QFConnection connection, 2:bool autoCommit) throws (1:QFSQLException ouch)
   bool connection_getAutoCommit(1:QFConnection connection) throws (1:QFSQLException ouch)
   void connection_setTransactionIsolation(1:QFConnection connection, 2:i32 level) throws (1:QFSQLException ouch)
   i32 connection_getTransactionIsolation(1:QFConnection connection) throws (1:QFSQLException ouch)
   void connection_setReadOnly(1:QFConnection connection, 2:bool readOnly) throws (1:QFSQLException ouch)
   bool connection_getReadOnly(1:QFConnection connection) throws (1:QFSQLException ouch)

   void connection_setCatalog(1:QFConnection connection, 2:string catalog) throws (1:QFSQLException ouch)
   string connection_getCatalog(1:QFConnection connection) throws (1:QFSQLException ouch)
   void connection_setSchema(1:QFConnection connection, 2:string schema) throws (1:QFSQLException ouch)
   string connection_getSchema(1:QFConnection connection) throws (1:QFSQLException ouch)

   string connection_getCatalogSeparator(1:QFConnection connection),
   string connection_getCatalogTerm(1:QFConnection connection),
   string connection_getSchemaTerm(1:QFConnection connection),

   QFResultSet connection_getCatalogs(1:QFConnection connection),
   QFResultSet connection_getSchemas(1:QFConnection connection, 2:string catalog, 3:string schemaPattern) throws (1:QFSQLException ouch)
   QFResultSet connection_getTables(1:QFConnection connection, 2:string catalog, 3:string schemaPattern, 4:string tableNamePattern, 5:list<string> types),
   QFResultSet connection_getColumns(1:QFConnection connection, 2:string catalog, 3:string schemaPattern, 4:string tableNamePattern, 5:string columnNamePattern),
   string connection_getSQLKeywords(1:QFConnection connection),
   QFResultSet connection_getTableTypes(1:QFConnection connection),

   QFResultSet connection_getTypeInfo(1:QFConnection connection) throws (1:QFSQLException ouch)

   void closeConnection(1:QFConnection connection) throws (1:QFSQLException ouch)

   void statement_close(1:QFStatement statement) throws (1:QFSQLException ouch)
   bool statement_execute(1:QFStatement statement, 2:string sql) throws (1:QFSQLException ouch)
   QFResultSet statement_executeQuery(1:QFStatement statement, 2:string sql) throws (1:QFSQLException ouch)
   QFResultSet statement_getResultSet(1:QFStatement statement) throws (1:QFSQLException ouch)
   i32 statement_getUpdateCount(1:QFStatement statement),
   i32 statement_getResultSetType(1:QFStatement statement)
   void statement_cancel(1:QFStatement statement) throws (1:QFSQLException ouch)

   statement_getWarnings_return statement_getWarnings(1:QFStatement statement) throws (1:QFSQLException ouch)
   void statement_clearWarnings(1:QFStatement statement) throws (1:QFSQLException ouch)

   i32 statement_getMaxRows(1:QFStatement statement) throws (1:QFSQLException ouch)
   void statement_setMaxRows(1:QFStatement statement, 2:i32 max) throws (1:QFSQLException ouch)
   i32 statement_getQueryTimeout(1:QFStatement statement) throws (1:QFSQLException ouch)
   void statement_setQueryTimeout(1:QFStatement statement, 2:i32 seconds) throws (1:QFSQLException ouch)

   void preparedStatement_close(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)
   bool preparedStatement_execute(1:QFPreparedStatement statement, 2:string sql) throws (1:QFSQLException ouch)
   QFResultSet preparedStatement_executeQuery(1:QFPreparedStatement statement, 2:string sql) throws (1:QFSQLException ouch)
   QFResultSet preparedStatement_getResultSet(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)
   i32 preparedStatement_getUpdateCount(1:QFPreparedStatement statement),
   i32 preparedStatement_getResultSetType(1:QFPreparedStatement statement)
   void preparedStatement_cancel(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)

   preparedStatement_getWarnings_return preparedStatement_getWarnings(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)
   void preparedStatement_clearWarnings(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)

   i32 preparedStatement_getMaxRows(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)
   void preparedStatement_setMaxRows(1:QFPreparedStatement statement, 2:i32 max) throws (1:QFSQLException ouch)
   i32 preparedStatement_getQueryTimeout(1:QFPreparedStatement statement) throws (1:QFSQLException ouch)
   void preparedStatement_setQueryTimeout(1:QFPreparedStatement statement, 2:i32 seconds) throws (1:QFSQLException ouch)
}
