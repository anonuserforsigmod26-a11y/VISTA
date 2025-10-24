#include "postgres_connection.hpp"
#include "postgres_binary_reader.hpp"

namespace duckdb {

void PostgresConnection::BeginCopyFrom(PostgresBinaryReader &reader, const string &query) {
	auto result = PQExecute(query.c_str());
	if (!result || PQresultStatus(result) != PGRES_COPY_OUT) {
		throw std::runtime_error("Failed to prepare COPY \"" + query + "\": " + string(PQresultErrorMessage(result)));
	}
	if (!reader.Next()) {
		throw IOException("Failed to fetch header for COPY \"%s\"", query);
	}
	reader.CheckHeader();
}

} // namespace duckdb
