#include "sdkd_internal.h"

#include <core/operations/document_view.hxx>
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

ViewExecutor::ViewExecutor(Handle* handle)
{
    this->handle = handle;
}

bool
ViewExecutor::executeView(Command cmd, ResultSet& out, const ResultOptions& options, const Request& req)
{
    string optstr;
    Error sdkd_err = 0;

    out.options = options;
    out.clear();
    this->rs = &out;

    Json::Value ctlopts = req.payload[CBSDKD_MSGFLD_DSREQ_OPTS];
    Json::Value vqopts = req.payload[CBSDKD_MSGFLD_V_QOPTS];
    int num_iterations = ctlopts[CBSDKD_MSGFLD_V_QITERCOUNT].asInt();
    int iterdelay = ctlopts[CBSDKD_MSGFLD_V_QDELAY].asInt();

    string dname = req.payload[CBSDKD_MSGFLD_V_DESNAME].asString();
    string vname = req.payload[CBSDKD_MSGFLD_V_MRNAME].asString();

    if (dname.size() == 0 || vname.size() == 0) {
        log_error("Design/ view names cannot be empty");
        return false;
    }

    std::optional<std::uint64_t> limit = vqopts["limit"].isUInt() ? std::optional<std::uint64_t>(vqopts["limit"].asUInt64()) : std::nullopt;
    std::optional<std::uint64_t> skip = vqopts["skip"].isUInt() ? std::optional<std::uint64_t>(vqopts["skip"].asUInt64()) : std::nullopt;

    handle->externalEnter();

    while (!handle->isCancelled()) {
        out.vresp_complete = false;

        rs->markBegin();

        std::vector<std::string> rows{};

        couchbase::core::operations::document_view_request request{};
        request.bucket_name = handle->options.bucket;
        request.document_name = dname;
        request.view_name = vname;
        request.limit = limit;
        request.skip = skip;
        // need to specify because of CXXCBC-88
        request.ns = couchbase::core::design_document_namespace::production;
        request.row_callback = [&rows](std::string&& row) {
            rows.emplace_back(std::move(row));
            return couchbase::core::utils::json::stream_control::next_row;
        };

        auto resp = handle->execute(request);

        out.setRescode(resp.ctx.ec);

        if (num_iterations >= 0) {
            num_iterations--;
            if (num_iterations <= 0) {
                break;
            }
        }

        if (iterdelay) {
            sdkd_millisleep(iterdelay);
        }
    }

    return true;
}
} // namespace CBSdkd
