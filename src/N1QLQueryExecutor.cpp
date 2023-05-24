#include "sdkd_internal.h"

namespace CBSdkd
{

bool
N1QLQueryExecutor::insertDoc(std::vector<std::string>& params, std::vector<std::string>& paramValues)
{
    auto pit = params.begin();
    auto vit = paramValues.begin();

    Json::Value doc;
    doc["id"] = std::to_string(handle->hid);

    for (; pit < params.end(); pit++, vit++) {
        doc[*pit] = *vit;
    }

    std::string val = Json::FastWriter().write(doc);
    auto value = couchbase::core::utils::to_binary(val);
    std::string key = doc["id"].asString();
    const auto [scope, collection] = handle->getCollection(key);

    couchbase::core::document_id id(handle->options.bucket, scope, collection, key);
    couchbase::core::operations::upsert_request req{ id, value };
    auto resp = handle->execute(req);

    if (resp.ctx.ec()) {
        return false;
    } else {
        mutation_tokens.emplace_back(resp.token);
        return true;
    }
}

bool
N1QLQueryExecutor::execute(Command cmd, ResultSet& out, const ResultOptions& options, const Request& req)
{

    int iterdelay = req.payload[CBSDKD_MSGFLD_HANDLE_OPTIONS][CBSDKD_MSGFLD_V_QDELAY].asInt();
    std::string consistency = req.payload[CBSDKD_MSGFLD_NQ_SCANCONSISTENCY].asString();
    std::string indexType = req.payload[CBSDKD_MSGFLD_NQ_INDEX_TYPE].asString();
    std::string indexEngine = req.payload[CBSDKD_MSGFLD_NQ_INDEX_ENGINE].asString();
    std::string indexName = req.payload[CBSDKD_MSGFLD_NQ_DEFAULT_INDEX_NAME].asString();
    std::string preparedStr = req.payload[CBSDKD_MSGFLD_NQ_PREPARED].asString();
    std::string flexStr = req.payload[CBSDKD_MSGFLD_NQ_FLEX].asString();
    bool prepared;
    bool flex;
    istringstream(preparedStr) >> std::boolalpha >> prepared;
    istringstream(flexStr) >> std::boolalpha >> flex;

    std::string scope = "0";
    std::string collection = "0";

    std::vector<std::string> params;
    std::vector<std::string> paramValues;
    N1QL::split(req.payload[CBSDKD_MSGFLD_NQ_PARAM].asString(), ',', params);
    N1QL::split(req.payload[CBSDKD_MSGFLD_NQ_PARAMVALUES].asString(), ',', paramValues);
    std::string scanConsistency = req.payload[CBSDKD_MSGFLD_NQ_SCANCONSISTENCY].asString();
    params.emplace_back("handleid");
    paramValues.push_back(std::to_string(handle->hid));

    int ii = 0;
    params.emplace_back("unique_id");
    paramValues.push_back(std::to_string(ii));

    out.clear();

    handle->externalEnter();

    while (!handle->isCancelled()) {
        out.query_resp_count = 0;
        paramValues.pop_back();
        paramValues.push_back(std::to_string(ii));

        insertDoc(params, paramValues);
        out.scan_consistency = scanConsistency;

        std::string q = std::string("select * from `") + this->handle->options.bucket + "`";
        if (handle->options.useCollections) {
            q = std::string("select * from `") + collection + "`";
        }

        if (indexType == "secondary") {
            q += std::string(" where ");
            bool isFirst = true;
            auto pit = params.begin();
            auto vit = paramValues.begin();

            for (; pit != params.end(); pit++, vit++) {
                if (!isFirst) {
                    q += std::string(" and ");
                } else {
                    isFirst = false;
                }
                q += *pit + std::string("=\"") + *vit + std::string("\" ");
            }
        }

        out.markBegin();

        std::vector<std::string> rows{};

        couchbase::core::operations::query_request request{};
        request.adhoc = !prepared;
        request.flex_index = flex;
        if (handle->options.useCollections && !handle->options.bucket.empty()) {
            request.query_context = fmt::format("default:`{}`.`{}`", handle->options.bucket, scope);
        }
        request.statement = q;
        request.row_callback = [&rows](std::string&& row) {
            rows.emplace_back(std::move(row));
            return couchbase::core::utils::json::stream_control::next_row;
        };

        if (scanConsistency == "request_plus") {
            request.scan_consistency = couchbase::query_scan_consistency::request_plus;
        } else if (scanConsistency == "at_plus") {
            request.mutation_state = mutation_tokens;
        } else {
            request.scan_consistency = couchbase::query_scan_consistency::not_bounded;
        }

        auto resp = handle->execute(request);

        out.query_resp_count = rows.size();
        out.setRescode(resp.ctx.ec, true);

        ii++;
    }
    return true;
}
} // namespace CBSdkd
