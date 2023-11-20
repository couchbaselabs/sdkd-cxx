#include "sdkd_internal.h"

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
using namespace std;

#define VIEWLOAD_BATCH_COUNT 1000

ViewLoader::ViewLoader(Handle* handle)
{
    this->handle = handle;
}

void
ViewLoader::flushValues(ResultSet& rs)
{
    std::vector<std::future<couchbase::core::operations::upsert_response>> futures{};
    for (auto& value : values) {
        auto collection = handle->getCollection(value.key);
        couchbase::core::document_id id(handle->options.bucket, collection.first, collection.second, value.key);
        auto v = couchbase::core::utils::to_binary(value.value);
        couchbase::core::operations::upsert_request req{ id, v };
        auto f = handle->execute_async(req);
        futures.emplace_back(std::move(f));
    }
    for (auto& future : futures) {
        auto resp = future.get();
        if (resp.ctx.ec()) {
            rs.setRescode(resp.ctx.ec());
        }
    }
}

bool
ViewLoader::populateViewData(Command cmd, const Dataset& ds, ResultSet& out, const ResultOptions& options, const Request& req)
{
    Json::Value schema = req.payload[CBSDKD_MSGFLD_V_SCHEMA];
    Json::FastWriter jwriter;

    out.clear();
    out.options = options;

    int ii = 0;
    DatasetIterator* iter = ds.getIter();

    handle->externalEnter();

    for (ii = 0, iter->start(); iter->done() == false; iter->advance(), ii++) {

        schema[CBSDKD_MSGFLD_V_KIDENT] = iter->key();
        schema[CBSDKD_MSGFLD_V_KSEQ] = ii;
        _kvp kvp;
        kvp.key = iter->key();
        kvp.value = jwriter.write(schema);
        values.push_back(kvp);

        if (values.size() >= VIEWLOAD_BATCH_COUNT) {
            flushValues(out);
            values.clear();
        }
    }

    if (!values.empty()) {
        flushValues(out);
    }

    return true;
}

} // namespace CBSdkd
