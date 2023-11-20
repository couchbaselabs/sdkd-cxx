/*
 * Handle.h
 *
 *  Created on: May 11, 2012
 *      Author: mnunberg
 */

#ifndef HANDLE_H_
#define HANDLE_H_

#include "Collections.h"

#ifndef SDKD_INTERNAL_H_
#error "include sdkd_internal.h first"
#endif

#include <future>

namespace CBSdkd {
using std::map;


class Handle;
class ResultOptions;

class MutateParams {
public:
    std::string oper;
    unsigned int expiry;
};

class Handle;



class HandleOptions {
public:
    HandleOptions() { }
    virtual ~HandleOptions() { }

    HandleOptions(const Json::Value& json) :
        hostname(json[CBSDKD_MSGFLD_HANDLE_HOSTNAME].asString()),
        username(""),
        password(""),
        bucket(json[CBSDKD_MSGFLD_HANDLE_BUCKET].asString())
    {
        const Json::Value& opts = json[CBSDKD_MSGFLD_HANDLE_OPTIONS];
        if ( opts.isObject() ) {
            timeout = opts[CBSDKD_MSGFLD_HANDLE_OPT_TMO].asUInt();
            username = opts[CBSDKD_MSGFLD_HANDLE_USERNAME].asString();
            password = opts[CBSDKD_MSGFLD_HANDLE_PASSWORD].asString();
            useSSL = opts[CBSDKD_MSGFLD_HANDLE_OPT_SSL].asBool();

            std::string collectionsType = json[CBSDKD_MSGFLD_HANDLE_COLLECTIONS_TYPE].asString();
            useCollections = collectionsType.compare("many") == 0;
            scopes = json[CBSDKD_MSGFLD_HANDLE_SCOPES].asUInt();
            collections = json[CBSDKD_MSGFLD_HANDLE_COLLECTIONS].asUInt();


            if (useSSL == true) {
                std::string clusterCert = opts[CBSDKD_MSGFLD_HANDLE_OPT_CLUSTERCERT].asString();

                if (!clusterCert.size()) {
                    fprintf(stderr, "SSL specified but no cert found");
                    exit(1);
                }

#ifndef _WIN32
                char pathtmp[] = "certXXXXXX";
                char *path = mktemp(pathtmp);
                if (!path) {
                    fprintf(stderr, "Unable to create path to store cert");
                    exit(1);
                }
                FILE *fp =  fopen(path, "w");
                fprintf(fp, "%s", clusterCert.c_str());
                fclose(fp);
                certpath = path;
#endif
                if (!certpath.size()) {
                    fprintf(stderr, "Unable to create cert path");
                    exit(1);
                }
            }

        } else {
            timeout = 0;
        }

    }

    bool isValid() {
        return (hostname.size() && bucket.size());
    }

    std::string hostname,
                username,
                password,
                bucket;
    bool useSSL, useCollections;
    std::string certpath;

    unsigned long timeout, scopes, collections;

};

class Handle : protected DebugContext {
public:

    Handle(const HandleOptions& options, couchbase::core::cluster cluster);

    template<class Request>
    auto
    execute(Request request)
    {
        auto f = execute_async(request);
        auto resp = f.get();
        return resp;
    }

    template<class Request>
    [[nodiscard]] auto execute_async(Request request)
    {
        using response_type = typename Request::response_type;
        auto barrier = std::make_shared<std::promise<response_type>>();
        auto f = barrier->get_future();
        cluster.execute(request, [barrier](response_type resp) { barrier->set_value(std::move(resp)); });
        return f;
    }

    template<class Request>
    [[nodiscard]] std::future<std::error_code> execute_async_ec(Request request)
    {
        using response_type = typename Request::response_type;
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.execute(request, [barrier](response_type resp) { barrier->set_value(std::move(resp.ctx.ec())); });
        return f;
    }

    template<class Handler>
    void drainPendingFutures(Handler handler) {
        for (auto &f : pending_futures) {
            auto ec = f.get();
            handler(ec);
        }
        pending_futures.clear();
    }

    bool
    dsGet(Command cmd,
          Dataset const& ds, ResultSet& out,
          ResultOptions const& options = ResultOptions());

    bool
    dsKeyop(Command cmd,
            Dataset const& ds, ResultSet& out,
            ResultOptions const& options = ResultOptions());

    bool
    dsMutate(Command cmd,
             Dataset const&, ResultSet& out,
             ResultOptions const& options = ResultOptions());


    // bool
    // dsEndure(Command cmd,
    //          Dataset const&, ResultSet& out,
    //          ResultOptions const& options = ResultOptions());

    // bool
    // dsEndureWithSeqNo(Command cmd,
    //          Dataset const&, ResultSet& out,
    //          ResultOptions const& options = ResultOptions());

    // bool
    // dsObserve(Command cmd,
    //          Dataset const&, ResultSet& out,
    //          ResultOptions const& options = ResultOptions());

    // bool
    // dsGetReplica(Command cmd,
    //          Dataset const&, ResultSet& out,
    //          ResultOptions const& options = ResultOptions());

    // bool
    // dsVerifyStats(Command cmd,
    //          Dataset const&, ResultSet& out,
    //          ResultOptions const& options = ResultOptions());

    bool
    dsSDSinglePath(Command cmd,
             Dataset const&, ResultSet& out,
             ResultOptions const& options = ResultOptions());

    void appendError(int err, const char *desc) {
        pending_errors.push_back(Error(err, desc));
    }

    couchbase::core::cluster getLcb() {
        return cluster;
    }

    // Helper Functions for cancellation
    bool isCancelled() {
        return do_cancel;
    }
    void externalEnter() {
        do_cancel = false;
    }

    // Cancels the current operation, causing it to return during the next
    // operation.
    void cancelCurrent();


    static void VersionInfoJson(Json::Value& res);
    HandleOptions options;
    unsigned long int hid;

    bool generateCollections();
    std::pair<string, string> getCollection(std::string key);

    std::vector<std::future<std::error_code>> pending_futures{};

private:
    bool do_cancel;

    std::vector<ResultSet>pending_results;
    std::vector<Error>pending_errors;

    couchbase::core::cluster cluster;
    Collections *collections;

    std::string certpath;
    void collect_result(ResultSet& rs);
    bool postsubmit(ResultSet& rs, unsigned int nsubmit = 1);
};


} /* namespace CBSdkd */
#endif /* HANDLE_H_ */
