#include "storage/postgres_transaction.hpp"
#include "storage/postgres_catalog.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "postgres_result.hpp"

extern "C" {
void *VistaImportSnapshot(const char *idstr);
int VistaGetSnapshotName(char *buf, uint32_t* generation);
int VistaFreeSnapshot(void *snapshot);
void vista_wrapper_external_unmap(void);
void vista_set_oldest_olap_generation(void *shrd_metadata_ptr, uint32_t olap_current_generation);
void *VistaGetShmem(void);
}

namespace duckdb {

PostgresTransaction::PostgresTransaction(PostgresCatalog &postgres_catalog, TransactionManager &manager,
                                         ClientContext &context)
    : Transaction(manager, context), access_mode(postgres_catalog.access_mode) {
	connection = postgres_catalog.GetConnectionPool().GetConnection();
}

PostgresTransaction::~PostgresTransaction() = default;

void PostgresTransaction::Start() {
	char snapshot_name[1024];
	uint32_t olap_generation = 0;
	transaction_state = PostgresTransactionState::TRANSACTION_NOT_YET_STARTED;
	if (VistaGetSnapshotName(snapshot_name, &olap_generation)) {
		// Set a random name if we could not get a valid snapshot name.
		snprintf(snapshot_name, 1024, "invalid");
	}

	// We need to get a snapshot for this transaction.
	this->snapshot = VistaImportSnapshot(snapshot_name);
	this->olap_generation = olap_generation;
}
void PostgresTransaction::Commit() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		if (this->snapshot) {
			vista_set_oldest_olap_generation(VistaGetShmem(), this->olap_generation);
			vista_wrapper_external_unmap();
			// We need to release the snapshot.
			VistaFreeSnapshot(this->snapshot);
			this->snapshot = NULL;
			
		}
		GetConnectionRaw().Execute("COMMIT");
	}
}
void PostgresTransaction::Rollback() {
	if (transaction_state == PostgresTransactionState::TRANSACTION_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_FINISHED;
		if (this->snapshot) {
			vista_set_oldest_olap_generation(VistaGetShmem(), this->olap_generation);
			vista_wrapper_external_unmap();
			// We need to release the snapshot.
			VistaFreeSnapshot(this->snapshot);
			this->snapshot = NULL;
		}
		GetConnectionRaw().Execute("ROLLBACK");
		
	}
}

static string GetBeginTransactionQuery(AccessMode access_mode) {
	string result = "BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ";
	if (access_mode == AccessMode::READ_ONLY) {
		result += " READ ONLY";
	}
	return result;
}

PostgresConnection &PostgresTransaction::GetConnection() {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string query = GetBeginTransactionQuery(access_mode);
		con.Execute(query);
	}
	return con;
}

PostgresConnection &PostgresTransaction::GetConnectionRaw() {
	return connection.GetConnection();
}

string PostgresTransaction::GetDSN() {
	return GetConnectionRaw().GetDSN();
}

unique_ptr<PostgresResult> PostgresTransaction::Query(const string &query) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery(access_mode);
		transaction_start += ";\n";
		return con.Query(transaction_start + query);
	}
	return con.Query(query);
}

vector<unique_ptr<PostgresResult>> PostgresTransaction::ExecuteQueries(const string &queries) {
	auto &con = GetConnectionRaw();
	if (transaction_state == PostgresTransactionState::TRANSACTION_NOT_YET_STARTED) {
		transaction_state = PostgresTransactionState::TRANSACTION_STARTED;
		string transaction_start = GetBeginTransactionQuery(access_mode);
		transaction_start += ";\n";
		return con.ExecuteQueries(transaction_start + queries);
	}
	return con.ExecuteQueries(queries);
}

string PostgresTransaction::GetTemporarySchema() {
	if (temporary_schema.empty()) {
		auto result = Query("SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema();");
		if (result->Count() < 1) {
			// no temporary tables exist yet in this connection
			// create a random temporary table and return
			Query("CREATE TEMPORARY TABLE __internal_temporary_table(i INTEGER)");
			result = Query("SELECT nspname FROM pg_namespace WHERE oid = pg_my_temp_schema();");
			if (result->Count() < 1) {
				throw BinderException("Could not find temporary schema pg_temp_NNN for this connection");
			}
		}
		temporary_schema = result->GetString(0, 0);
	}
	return temporary_schema;
}

PostgresTransaction &PostgresTransaction::Get(ClientContext &context, Catalog &catalog) {
	return Transaction::Get(context, catalog).Cast<PostgresTransaction>();
}

} // namespace duckdb
