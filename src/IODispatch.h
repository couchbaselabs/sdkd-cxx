/*
 * IODispatch.h
 *
 *  Created on: May 12, 2012
 *      Author: mnunberg
 */

#ifndef IODISPATCH_H_
#define IODISPATCH_H_
#ifndef SDKD_INTERNAL_H_
#error "Include sdkd_internal.h first"
#endif

#include "sdkd_internal.h"

namespace CBSdkd {

extern int g_pFactor;

typedef unsigned long int cbsdk_hid_t;

class IOProtoHandler : protected DebugContext {
public:
    enum IOStatus {
        OK,
        NOTYET,
        ERR
    };

    IOProtoHandler(sdkd_socket_t newsock) : DebugContext(), sockfd(newsock) { }
    IOProtoHandler() : DebugContext(), sockfd(INVALID_SOCKET), inbuf("") { }

    virtual ~IOProtoHandler()  {
        if (sockfd != INVALID_SOCKET) {
            closesocket(sockfd);
            sockfd = INVALID_SOCKET;
        }
    }

    IOStatus getRawMessage(std::string& msgbuf);
    void writeResponse(const Response& res);
    int readRequest(Request**);

protected:
    // Main communication socket.
    // For the main dispatch, this is the control handle;
    // For a worker, this is the single communication socket.
    sdkd_socket_t sockfd;

    int flushBuffer();

    // Flush the buffer, but block
    int flushBufferBlock();
    void setupFdSets(fd_set *rd, fd_set *wr, fd_set *exc)
    {
        FD_ZERO(rd);
        FD_ZERO(wr);
        FD_ZERO(exc);

        FD_SET(sockfd, rd);
        FD_SET(sockfd, exc);

        if (outbuf.size()) {
            FD_SET(sockfd, wr);
        }
    }

    // Input/output buffers for the main socket
    std::string inbuf;
    std::string outbuf;
    std::list<std::string> newlines;

private:
    int readSocket();
};

class IODispatch : protected IOProtoHandler {

public:
    IODispatch();
    virtual ~IODispatch();

    virtual void run() = 0;


};

class UsageCollector;
class MainDispatch;
class WorkerDispatch;
class WorkerHandle;

class MainDispatch : public IODispatch {

public:
    static const int max_io_threads_supported_by_sdk {1};

    MainDispatch();
    virtual ~MainDispatch();

    bool establishSocket(struct sockaddr_in *addr);
    void run();

    void registerWDHandle(cbsdk_hid_t, WorkerDispatch*);
    void unregisterWDHandle(cbsdk_hid_t);

    const Dataset* getDatasetById(const std::string& dsid);
    std::string uploadLogs(Json::Value payload);

    std::error_code ensureCluster(couchbase::core::origin origin, const std::string& bucket);
    couchbase::core::cluster getCluster();

  private:
    sdkd_socket_t acceptfd{-1};

    void create_new_ds(const Request* req);

    // Map from a DSID to a Dataset
    std::map<std::string,Dataset*> dsmap;

    // Map from a Handle ID to a handle
    std::map<cbsdk_hid_t, WorkerDispatch*> h2wmap;

    std::list<WorkerDispatch*> children;
    Mutex *dsmutex;
    Mutex *wmutex;


    void _collect_workers();
    const Handle* _get_handle(cbsdk_hid_t);

    void dispatch_cancel(const Request&);
    bool loopOnce();
    bool dispatchCommand(Request*);
    bool isCollectingStats;
    UsageCollector *coll;

    std::vector<std::thread> io_threads{};
    asio::io_context io{};
    couchbase::core::cluster cluster{io};
    bool cluster_initialized{ false };
};


#define SDKD_INIT_WORKER_GLOBALS()
class WorkerDispatch : protected IODispatch {

public:
    friend class MainDispatch;
    WorkerDispatch(sdkd_socket_t newsock, MainDispatch* parent);
    virtual ~WorkerDispatch();

    void run();

    // Cancels the current running handle, if any
    void cancelCurrentHandle();

    Thread *thr;
    unsigned int id;

private:
    MainDispatch *parent;
    char *friendly_cstr;

    Handle *cur_handle;
    cbsdk_hid_t cur_hid;

    //ResultSet persistRs;

    bool initializeHandle(const Request&);
    bool processRequest(const Request&);
    bool selectLoop();

    static void *pthr_run(WorkerDispatch *w);
    ResultSet *rs;

    Mutex *hmutex;
};

} /* namespace CBSdkd */
#endif /* IODISPATCH_H_ */
