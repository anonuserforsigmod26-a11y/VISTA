#include "storage/postgres_catalog_set.hpp"
#include "storage/postgres_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/postgres_schema_entry.hpp"

namespace duckdb {

PostgresCatalogSet::PostgresCatalogSet(Catalog &catalog, bool is_loaded_p) : catalog(catalog), is_loaded(is_loaded_p) {
}

optional_ptr<CatalogEntry> PostgresCatalogSet::GetEntry(ClientContext &context, const string &name) {
	{
		lock_guard<mutex> l(entry_lock);
		auto entry = entries.find(name);
		if (entry != entries.end()) {
			// entry found
			return entry->second.get();
		}
	}

	//! [VISTA] We don't need reload. (there was reloading function here.)

	// check the case insensitive map if there are any entries
	auto name_entry = entry_map.find(name);
	if (name_entry == entry_map.end()) {
		// no entry found
		return nullptr;
	}
	// try again with the entry we found in the case insensitive map
	auto entry = entries.find(name_entry->second);
	if (entry == entries.end()) {
		// still not found
		return nullptr;
	}
	return entry->second.get();
}

void PostgresCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	string drop_query = "DROP ";
	drop_query += CatalogTypeToString(info.type) + " ";
	if (info.if_not_found == OnEntryNotFound::RETURN_NULL) {
		drop_query += " IF EXISTS ";
	}
	if (!info.schema.empty()) {
		drop_query += KeywordHelper::WriteQuoted(info.schema, '"') + ".";
	}
	drop_query += KeywordHelper::WriteQuoted(info.name, '"');
	if (info.cascade) {
		drop_query += "CASCADE";
	}
	auto &transaction = PostgresTransaction::Get(context, catalog);
	transaction.Query(drop_query);

	// erase the entry from the catalog set
	lock_guard<mutex> l(entry_lock);
	entries.erase(info.name);
}

void PostgresCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> PostgresCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("PostgresCatalogSet::CreateEntry called with empty name");
	}
	entry_map.insert(make_pair(result->name, result->name));
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void PostgresCatalogSet::ClearEntries() {
	entry_map.clear();
	entries.clear();
	is_loaded = false;
}

PostgresInSchemaSet::PostgresInSchemaSet(PostgresSchemaEntry &schema, bool is_loaded)
    : PostgresCatalogSet(schema.ParentCatalog(), is_loaded), schema(schema) {
}

optional_ptr<CatalogEntry> PostgresInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	entry->internal = schema.internal;
	return PostgresCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
