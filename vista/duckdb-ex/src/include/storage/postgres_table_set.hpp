//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class PostgresConnection;
class PostgresResult;
class PostgresSchemaEntry;

class PostgresTableSet : public PostgresInSchemaSet {
public:
	explicit PostgresTableSet(PostgresSchemaEntry &schema, unique_ptr<PostgresResultSlice> tables = nullptr);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<PostgresTableInfo> GetTableInfo(PostgresTransaction &transaction, PostgresSchemaEntry &schema,
	                                                  const string &table_name);
	static unique_ptr<PostgresTableInfo> GetTableInfo(PostgresConnection &connection, const string &schema_name,
	                                                  const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

	static string GetInitializeQuery(const string &schema = string(), const string &table = string());

	void LoadEntries(ClientContext &context) override;

protected:
	bool SupportReload() const override {
		return true;
	}

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(optional_ptr<PostgresTransaction> transaction, optional_ptr<PostgresSchemaEntry> schema,
	                      PostgresResult &result, idx_t row, PostgresTableInfo &table_info);
	static void AddConstraint(PostgresResult &result, idx_t row, PostgresTableInfo &table_info);
	static void AddColumnOrConstraint(optional_ptr<PostgresTransaction> transaction,
	                                  optional_ptr<PostgresSchemaEntry> schema, PostgresResult &result, idx_t row,
	                                  PostgresTableInfo &table_info);

	void CreateEntries(PostgresTransaction &transaction, PostgresResult &result, idx_t start, idx_t end);

private:
	string GetAlterTablePrefix(ClientContext &context, const string &name);
	string GetAlterTablePrefix(const string &name, optional_ptr<CatalogEntry> entry);
	string GetAlterTableColumnName(const string &name, optional_ptr<CatalogEntry> entry);

protected:
	unique_ptr<PostgresResultSlice> table_result;
};

} // namespace duckdb
