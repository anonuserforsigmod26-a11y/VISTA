//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"
#include "postgres_connection.hpp"
#include "storage/postgres_connection_pool.hpp"
#include <cstdio>

namespace duckdb {
class PostgresCatalog;
class PostgresSchemaEntry;
class PostgresTableEntry;

enum class PostgresTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class PostgresTransaction : public Transaction {
public:
	PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager, ClientContext &context);
	~PostgresTransaction() override;

	void Start();
	void Commit();
	void Rollback();

	PostgresConnection &GetConnection();
	string GetDSN();
	unique_ptr<PostgresResult> Query(const string &query);
	vector<unique_ptr<PostgresResult>> ExecuteQueries(const string &queries);
	static PostgresTransaction &Get(ClientContext &context, Catalog &catalog);
	void *GetSnapshot() { return snapshot; }

	string GetTemporarySchema();

private:
	PostgresPoolConnection connection;
	PostgresTransactionState transaction_state;
	AccessMode access_mode;
	string temporary_schema;

private:
	//! Retrieves the connection **without** starting a transaction if none is active
	PostgresConnection &GetConnectionRaw();
	void *snapshot;
	uint32_t olap_generation;
};

} // namespace duckdb
