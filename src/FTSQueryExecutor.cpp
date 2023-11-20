#include "sdkd_internal.h"
#include <thread>

#include <core/operations/document_search.hxx>
#include <core/operations/document_get.hxx>
#include <core/operations/document_upsert.hxx>
#include <core/operations/document_insert.hxx>
#include <core/operations/document_remove.hxx>
#include <core/operations/document_touch.hxx>
#include <core/operations/document_lookup_in.hxx>
#include <core/operations/document_mutate_in.hxx>
#include <core/operations/document_replace.hxx>
#include <core/operations/document_append.hxx>
#include <core/operations/document_prepend.hxx>
#include <core/operations/document_increment.hxx>
#include <core/operations/document_decrement.hxx>

namespace CBSdkd
{

std::vector<std::string>
collectionsForSearch(int numOfCollections)
{
    std::vector<std::string> collections{};
    for (int i = 0; i < numOfCollections; i++) {
        collections.emplace_back(std::to_string(i));
    }
    return collections;
}

std::error_code
FTSQueryExecutor::runSearchOnPreloadedData(ResultSet& out, std::string& indexName, int kvCount)
{
    out.markBegin();
    out.fts_query_resp_count = 0;

    generator = (generator + 1) % (2 * kvCount);

    Json::Value query;
    std::string searchField =
      generator % 2 == 0 ? "SampleValue" + std::to_string(generator / 2) : "SampleSubvalue" + std::to_string(generator / 2);
    query["match"] = searchField;

    std::vector<std::string> rows{};

    couchbase::core::operations::search_request req{};
    req.index_name = indexName;
    req.query = couchbase::core::json_string(Json::FastWriter().write(query));
    if (numOfCollections != 0) {
        req.collections = collectionsForSearch(numOfCollections);
    }
    req.row_callback = [&rows](std::string&& row) {
        rows.emplace_back(std::move(row));
        return couchbase::core::utils::json::stream_control::next_row;
    };

    auto resp = handle->execute(req);

    out.fts_query_resp_count = rows.size();

    return resp.ctx.ec;
}

std::error_code
FTSQueryExecutor::runSearchUnderAtPlusConsistency(ResultSet& out, std::string& indexName)
{
    out.markBegin();
    out.fts_query_resp_count = 0;

    std::stringstream sv;
    sv << "SampleValue:" << std::this_thread::get_id() << ":" << std::to_string(generator);
    std::string match_term = sv.str();

    Json::Value doc;
    doc["value"] = match_term;
    std::string curv = Json::FastWriter().write(doc);
    std::stringstream ss;
    ss << "key" << std::this_thread::get_id() << ":";
    std::string curk = ss.str();
    if (generator % 2 == 0)
        curk += std::to_string(generator);
    else
        curk += std::to_string(generator / 2);

    pair<string, string> collection = handle->getCollection(curk);

    std::vector<couchbase::mutation_token> mutation_state{};

    {
        couchbase::core::document_id id(handle->options.bucket, collection.first, collection.second, curk);
        auto value = couchbase::core::utils::to_binary(curv);
        couchbase::core::operations::upsert_request req{ id, value };
        auto resp = handle->execute(req);
        if (resp.ctx.ec()) {
            return resp.ctx.ec();
        } else {
            mutation_state.emplace_back(resp.token);
        }
    }

    Json::Value query;
    std::string searchField = match_term;
    query["match"] = searchField;

    couchbase::core::operations::search_request req{};
    req.index_name = indexName;
    req.query = couchbase::core::json_string(Json::FastWriter().write(query));
    if (numOfCollections != 0) {
        req.collections = collectionsForSearch(numOfCollections);
    }
    req.mutation_state = mutation_state;

    auto resp = handle->execute(req);

    out.fts_query_resp_count = resp.rows.size();

    generator++;
    return resp.ctx.ec;
}

bool
FTSQueryExecutor::execute(ResultSet& out, const ResultOptions& options, const Request& req)
{
    out.clear();
    handle->externalEnter();

    std::string indexName = req.payload[CBSDKD_MSGFLD_FTS_INDEXNAME].asString();
    unsigned int kvCount = req.payload[CBSDKD_MSGFLD_FTS_COUNT].asInt64();
    numOfCollections = req.payload[CBSDKD_MSGFLD_FTS_COLLECTIONS].asInt64();

    // Check consistency level of fts query
    bool not_bounded = true;
    if (strcmp(req.payload[CBSDKD_MSGFLD_FTS_CONSISTENCY].asCString(), "at_plus") == 0)
        not_bounded = false;
    log_info("running fts queries with not_bounded = %d", not_bounded);

    while (!handle->isCancelled()) {
        std::error_code ec;
        if (not_bounded) {
            ec = runSearchOnPreloadedData(out, indexName, kvCount);
        } else {
            ec = runSearchUnderAtPlusConsistency(out, indexName);
        }

        out.setRescode(ec, true);
    }
    return true;
}
} // namespace CBSdkd
