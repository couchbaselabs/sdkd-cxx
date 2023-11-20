#ifndef SDKD_N1QL_H
#define SDKD_N1QL_H
#endif

#ifndef SDKD_INTERNAL_H_
#error "include sdkd_internal.h first"
#endif

#include "sdkd_internal.h"

#include <couchbase/mutation_token.hxx>

namespace CBSdkd {

class N1QL : protected DebugContext {
public:
    N1QL(Handle *handle) :
        handle(handle) {
    }
    ~N1QL() {}

    void split(const std::string &s, char delim, std::vector<std::string> &elems) {
        std::stringstream ss(s);
        std::string item;

        while(std::getline(ss, item, delim)) {
            if (!item.empty()) {
                elems.push_back(item);
            }
        }
    };

private:
    Handle *handle;
};

class N1QLQueryExecutor : public N1QL {
public:
    N1QLQueryExecutor(Handle *handle) :
        N1QL(handle), is_isuccess(false),
         handle(handle) {
    }
    ~N1QLQueryExecutor() {}

    bool execute(Command cmd,
            ResultSet& s,
            const ResultOptions& opts,
            const Request& req);

    bool is_isuccess;
    bool query;
    std::vector<couchbase::mutation_token> mutation_tokens{};
private:
    bool insertDoc(std::vector<std::string>& params,
            std::vector<std::string>& paramValues);

    Handle *handle;
    void split(const std::string& str, char delim, std::vector<std::string>& elems);
};

class N1QLLoader : public N1QL {
public:
    N1QLLoader(Handle *handle) :
        N1QL(handle), handle(handle) {
    }
    bool populate(const Dataset& ds);
private:
    Handle *handle;

};
}
