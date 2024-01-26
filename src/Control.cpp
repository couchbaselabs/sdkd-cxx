#include "sdkd_internal.h"

namespace CBSdkd
{

MainDispatch::MainDispatch()
  : IODispatch()
{
    setLogPrefix("LCB SDKD Control");
    dsmutex = Mutex::Create();
    wmutex = Mutex::Create();
    isCollectingStats = false;
    for (int i = 0; i < 4; i++) {
        io_threads.emplace_back(std::thread([this]() { io.run(); }));
    }
}

MainDispatch::~MainDispatch()
{
    if (acceptfd >= 0) {
        closesocket(acceptfd);
        acceptfd = -1;
    }

    log_info("Bye Bye!");
    while (children.size()) {
        for (std::list<WorkerDispatch*>::iterator iter = children.begin();
                iter != children.end(); iter++) {

            (*iter)->cancelCurrentHandle();
        }
        _collect_workers();
    }

    {
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        cluster.close([barrier]() { barrier->set_value(); });
        f.get();
    }

    for (auto& t : io_threads) {
        t.join();
    }

    delete wmutex;
    delete dsmutex;
}

void
MainDispatch::registerWDHandle(cbsdk_hid_t id, WorkerDispatch *d)
{
    wmutex->lock();
    h2wmap[id] = d;
    wmutex->unlock();
}

void
MainDispatch::unregisterWDHandle(cbsdk_hid_t id)
{
    wmutex->lock();
    h2wmap.erase(id);
    wmutex->unlock();
}

std::error_code
MainDispatch::ensureCluster(couchbase::core::origin origin, const std::string& bucket) {
    wmutex->lock();
    if (cluster_initialized) {
        wmutex->unlock();
        return {};
    }

    std::error_code ec{};

    {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open(origin, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        ec = f.get();
        CB_LOG_TRACE("open cluster: {}", ec.message());
    }

    if (!ec) {
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        cluster.open_bucket(bucket, [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
        ec = f.get();
        CB_LOG_TRACE("open bucket \"{}\": {}", bucket, ec.message());
    }

    if (!ec) {
        cluster_initialized = true;
    }

    wmutex->unlock();

    return ec;
}

couchbase::core::cluster MainDispatch::getCluster() {
    return cluster;
}

bool
MainDispatch::establishSocket(struct sockaddr_in *addr)
{
    acceptfd = sdkd_start_listening(addr);
    if (acceptfd == INVALID_SOCKET) {
        return false;
    }
    return true;
}

extern "C" {
#ifdef _WIN32
static unsigned __stdcall new_worker_thread(void *worker)
{
    reinterpret_cast<WorkerDispatch*>(worker)->run();
    return 0;
}
#else
static void *new_worker_thread(void *worker)
{
    ((WorkerDispatch*)worker)->run();
    return NULL;
}
#endif

}

void
MainDispatch::_collect_workers()
{
    std::list<WorkerDispatch*>::iterator iter = children.begin();
    while (iter != children.end()) {
        WorkerDispatch *w = *iter;
        if (!w->thr->isAlive()) {
            w->thr->join();

            log_debug("Joined thread '%s'", w->getLogPrefix().c_str());
            delete w;

            children.erase(iter++);
        } else {
            iter++;
        }
    }
}

static void removeTrailingWhitespace(std::string &str) {
    auto last_non_whitespace = std::find_if(str.rbegin(), str.rend(), [](unsigned char c) { return !std::isspace(c); });
    str.erase(last_non_whitespace.base(), str.end());
}

bool
MainDispatch::dispatchCommand(Request *reqp)
{
    if (reqp->command == Command::NEWDATASET) {
        create_new_ds(reqp);

    } else if (reqp->command == Command::GOODBYE) {
        if (isCollectingStats == true) {
            coll->StopCollector();
            Response res = Response(reqp);
            Json::Value statsres;
            coll->GetResponseJson(statsres);
            res.setResponseData(statsres);
            writeResponse(res);
        }
        return false;
    } else if (reqp->command == Command::CANCEL) {
        WorkerDispatch *w = h2wmap[reqp->handle_id];
        if (!w) {
            writeResponse(Response(reqp,
                                   Error(Error::SUBSYSf_SDKD,
                                         Error::SDKD_ENOHANDLE)));
        } else {
            w->cancelCurrentHandle();
            writeResponse(Response(reqp, 0));
        }

    } else if (reqp->command == Command::INFO) {
        g_pFactor = reqp->payload["PercentileFactor"].asInt();
        Response res = Response(reqp);
        Json::Value infores;
        Handle::VersionInfoJson(infores);
        res.setResponseData(infores);
        writeResponse(res);
        coll = new UsageCollector();
        coll->Start();
        isCollectingStats = true;
    } else if (reqp->command == Command::TTL) {
        if (!reqp->payload.isMember(CBSDKD_MSGFLD_TTL_SECONDS)) {
            writeResponse(Response(reqp,
                                   Error::createInvalid(
                                           "Missing Seconds")));
        } else {
            int seconds = reqp->payload[CBSDKD_MSGFLD_TTL_SECONDS].asInt();

            if (seconds < 0) {
                seconds = 0;
            }

            sdkd_set_ttl(seconds);
            writeResponse(Response(reqp, Error(0)));
        }
    } else if (reqp->command == Command::GETUSAGE) {
        coll = new UsageCollector();
        coll->Start();
        isCollectingStats = true;
    } else if (reqp->command == Command::UPLOADLOGS) {
        std::string url = uploadLogs(reqp->payload);
        removeTrailingWhitespace(url);
        Response res = Response(reqp);
        Json::Value val;
        val["url"] = url;
        res.setResponseData(val);
        writeResponse(res);
    } else {
    // We don't currently support other types of control messages
        writeResponse(Response(reqp,
                               Error(Error::SUBSYSf_SDKD,
                                     Error::SDKD_ENOIMPL)));
    }
    return true;
}

bool
MainDispatch::loopOnce()
{
    fd_set rfds;
    fd_set excfds;
    fd_set wfds;

    setupFdSets(&rfds, &wfds, &excfds);

    FD_SET(acceptfd, &rfds);
    FD_SET(acceptfd, &excfds);

    _collect_workers();

    sdkd_socket_t fd_max = max(acceptfd, sockfd) + 1;

    int selv = select((int)fd_max, &rfds, &wfds, &excfds, NULL);

    _collect_workers();

    if (selv == -1) {
        int errno_save = sdkd_socket_errno();
        if (errno_save != EINTR) {
            fprintf(stderr, "Got error %d on select()\n", errno_save);
            return false;
        }
    }

    if (FD_ISSET(acceptfd, &excfds)) {
        fprintf(stderr, "Accept fd has exception\n");
        return false;
    }

    if (FD_ISSET(sockfd, &excfds)) {
        fprintf(stderr, "Sockfd has exception\n");
        return false;
    }

    if (FD_ISSET(sockfd, &wfds)) {
        if (flushBufferBlock() == -1) {
            fprintf(stderr, "Got error writing data\n");
            return false;
        }
    }

    if (FD_ISSET(acceptfd, &rfds)) {
        // Establish new worker thread...
        struct sockaddr_in inaddr;
        sdkd_socket_t newsock = sdkd_accept_socket(acceptfd, &inaddr);

        if (newsock == INVALID_SOCKET) {
            printf("Problem accepting new socket!\n");
            return false;
        }

        // Check any children that might need to be cleaned up:
        WorkerDispatch *w = new WorkerDispatch(newsock, this);
        children.push_back(w);
        w->thr = Thread::Create(new_worker_thread);
        w->thr->start(w);
    }

    if (FD_ISSET(sockfd, &rfds)) {
        int rv;
        bool dispatch_rv = true;

        Request *reqp = NULL;
        rv = readRequest(&reqp);
        if (!reqp) {
            if (rv == -1) {
                fprintf(stderr, "Control socket errored\n");
                return false;
            }
            return true;
        }

        dispatch_rv = dispatchCommand(reqp);
        delete reqp;
        return dispatch_rv;
    }

    return true;
}

void
MainDispatch::run()
{
    // So we've already established the socket..
    assert(acceptfd != INVALID_SOCKET);

    // The first control connection is special
    struct sockaddr_in ctlconn;
    if (INVALID_SOCKET == (sockfd = sdkd_accept_socket(acceptfd, &ctlconn))) {
        return;  // Nothing to do here..
    }

    if (SOCKET_ERROR == sdkd_make_socket_nonblocking(sockfd, 1)) {
        return;
    }

    if (SOCKET_ERROR == sdkd_make_socket_nonblocking(acceptfd, 1)) {
        return;
    }

    while (loopOnce()) {
        ;
    }
    flushBufferBlock();
}

void
MainDispatch::create_new_ds(const Request *reqp)
{
    // It only makes sense to construct an invidual dataset
    // if we have an ID by which to refer to it later on
    std::string refid = "";
    Dataset::Type dstype = Dataset::determineType(*reqp, &refid);
    if (dstype == Dataset::DSTYPE_INVALID) {
        Response resp = Response(reqp,
                                 Error(Error::SUBSYSf_SDKD,
                                       Error::SDKD_EINVAL,
                                       "Invalid Dataset"));
        writeResponse(resp);
    }

    Dataset *ds = Dataset::fromType(dstype, *reqp);

    if (ds && ds->isValid() && refid.size()) {
        // Insert the DS
        dsmutex->lock();
        if (dsmap[refid]) {
            dsmutex->unlock();
            writeResponse(Response(reqp,
                                   Error(Error::SUBSYSf_SDKD,
                                         Error::SDKD_ENODS,
                                         "Dataset already exists with this ID")));
            delete ds;
            return;
        }
        dsmap[refid] = ds;
        dsmutex->unlock();

    } else {
        delete ds;
        writeResponse(Response(reqp,
                               Error(Error::SUBSYSf_SDKD,
                                     Error::SDKD_EINVAL,
                                     "NEWDATASET must have an ID")));
    }
}

const Dataset *
MainDispatch::getDatasetById(const std::string& dsid)
{
    dsmutex->lock();
    const Dataset *ret = dsmap[dsid];
    dsmutex->unlock();
    return ret;
}

std::string
MainDispatch::uploadLogs(Json::Value payload) {
    if (!payload[CBSDKD_MSGFLD_S3_BUCKET].isString() ||
        !payload[CBSDKD_MSGFLD_S3_ACCESSS].isString() ||
        !payload[CBSDKD_MSGFLD_S3_SECRET].isString()) {
        return "";
    }

    char command[1024], urlbuf[1024];
    memset(command, '\0', sizeof command);
    sprintf(command, "python3 pylib/s3upload.py %s %s %s %s cpp", payload[CBSDKD_MSGFLD_S3_BUCKET].asCString(), payload[CBSDKD_MSGFLD_S3_ACCESSS].asCString(), payload[CBSDKD_MSGFLD_S3_SECRET].asCString(), Daemon::MainDaemon->getOptions().lcblogFile);
    FILE *fp = popen(command, "r");
    memset(command, '\0', sizeof urlbuf);
    fgets(urlbuf, sizeof urlbuf, fp);
    return std::string(urlbuf);
}

} // namespace CBSdkd
