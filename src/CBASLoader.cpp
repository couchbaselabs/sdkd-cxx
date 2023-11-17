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
bool
CBASLoader::populate(const Dataset& ds)
{
    DatasetIterator* iter = ds.getIter();
    int batch = 100;
    int jj = 0;

    for (jj = 0, iter->start(); iter->done() == false; iter->advance(), jj++) {
        std::string k = iter->key();
        std::string v = iter->value();
        auto value = couchbase::core::utils::to_binary(v);
        pair<string, string> collection = handle->getCollection(k);

        couchbase::core::document_id id(handle->options.bucket, collection.first, collection.second, k);

        couchbase::core::operations::upsert_request req{ id, value };
        handle->pending_futures.emplace_back(handle->execute_async_ec(req));

        if (jj % batch == 0) {
            bool ok = true;
            handle->drainPendingFutures([&ok](std::error_code ec) { ok = ok && !ec; });
            if (!ok) {
                return false;
            }
        }
    }

    handle->drainPendingFutures([](std::error_code ec) {});

    delete iter;
    return true;
}

} // namespace CBSdkd
