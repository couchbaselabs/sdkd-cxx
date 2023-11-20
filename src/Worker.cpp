#include "sdkd_internal.h"

#include <core/utils/connection_string.hxx>

namespace CBSdkd {


WorkerDispatch::~WorkerDispatch()
{
    if (cur_handle) {
        delete cur_handle;
        cur_handle = NULL;
    }
    delete rs;
    delete hmutex;
    rs = NULL;
}

WorkerDispatch::WorkerDispatch(sdkd_socket_t newsock, MainDispatch *parent)
: IODispatch(),
  parent(parent),
  cur_handle(NULL),
  cur_hid(0)
{
    sockfd = newsock;
    stringstream ss;
    ss << "lcb-sdkd-worker fd=" << newsock;
    setLogPrefix(ss.str());
    hmutex = Mutex::Create();
    rs = new ResultSet(g_pFactor);
}

bool
WorkerDispatch::initializeHandle(const Request &req)
{
    HandleOptions hOpts = HandleOptions(req.payload);
    Error err = 0;

    if (!hOpts.isValid()) {
        err = Error(Error::SUBSYSf_SDKD, Error::SDKD_EINVAL,
                    "Problem with handle input");
        goto GT_ERR;
    }

    if (cur_handle != NULL) {
        err = Error(Error::SUBSYSf_SDKD, Error::SDKD_EINVAL,
                    "Handle already exists!");
        goto GT_ERR;
    }

    // create cluster if doesn't exist
    {
        // Gather parameters
        std::string connstr;

        if (hOpts.useSSL) {
            connstr += std::string("couchbases://") + hOpts.hostname;
            connstr += std::string("?trust_certificate=");
            connstr += std::string(hOpts.certpath);
        } else {
            connstr += std::string("couchbase://") + hOpts.hostname;
        }

        auto cb_connstr = couchbase::core::utils::parse_connection_string(connstr);

        couchbase::core::cluster_credentials auth{};
        auth.username = hOpts.username;
        auth.password = hOpts.password;

        auto origin = couchbase::core::origin(auth, cb_connstr);

        auto ec = parent->ensureCluster(origin, hOpts.bucket);

        if (ec) {
            err = Error(Error::SUBSYSf_SDKD, Error::SDKD_EINVAL, ec.message());
            goto GT_ERR;
        }
    }

    cur_handle = new Handle(hOpts, parent->getCluster());
    cur_hid = req.handle_id;
    cur_handle->hid =  cur_hid;

    parent->registerWDHandle(cur_hid, this);
    // if (!cur_handle->connect(&err)) {
    //     log_error("Couldn't establish initial LCB connection");
    // }
    if(!cur_handle->generateCollections()) {
        log_info("Didn't create collections"); //Not a collections test || Already created collections || Creation failed
    }

    GT_ERR:
    if (!err) {
        writeResponse(Response(req));

    } else {
        writeResponse(Response(req, err));
    }
    return true;
}

// Return false when the main loop should terminate. True otherwise.
bool
WorkerDispatch::processRequest(const Request& req)
{
    Error errp;
    const Dataset *ds;
    ResultSet rs(g_pFactor);

    Dataset::Type dstype;
    std::string refid;
    bool needs_ds = true;
    unique_ptr<const Dataset> ds_scopedel;
    ds_scopedel.reset();

    if (!req.isValid()) {
        log_warn("Got invalid request..");
        return false;
    }

    if (req.command == Command::CLOSEHANDLE) {
        log_info("CLOSEHANDLE called. Returning.");
        return false;
    }

    if (req.command == Command::NEWHANDLE) {
        return initializeHandle(req);
    }

    if (!cur_handle) {
        writeResponse(Response(req, Error(Error::SUBSYSf_SDKD,
                                          Error::SDKD_ENOHANDLE,
                                          "No handle created yet")));
        return true;
    }

    Handle& h = *cur_handle;
    rs.parent = cur_handle;

    if (req.command == Command::CB_VIEW_QUERY ||
            req.command == Command::CB_N1QL_QUERY ||
            req.command == Command::CB_N1QL_CREATE_INDEX ||
            req.command == Command::CB_FTS_QUERY ||
            req.command == Command::CB_AS_QUERY) {
        needs_ds = false;
    }

    if (needs_ds) {
        dstype = Dataset::determineType(req, &refid);
        if (dstype == Dataset::DSTYPE_INVALID) {
            errp.setCode(Error::SUBSYSf_SDKD|Error::SDKD_EINVAL);
            errp.setString("Couldn't parse dataset");
            abort();
            goto GT_CHECKERR;
        }

        if (dstype == Dataset::DSTYPE_REFERENCE) {
            // Assume it's valied
            ds = parent->getDatasetById(refid);
            if (!ds) {
                errp.setCode(Error::SUBSYSf_SDKD|Error::SDKD_ENODS);
                errp.setString("Couldn't find dataset");
                goto GT_CHECKERR;
            }
        } else {
            ds = Dataset::fromType(dstype, req);
            if (ds == NULL) {
                errp.setCode(Error::SUBSYSf_SDKD|Error::SDKD_EINVAL);
                errp.setString("Bad dataset parameters");
                goto GT_CHECKERR;
            }

            // Make the DS get cleaned up when we exit.
            ds_scopedel.reset(ds);
        }
    }

    GT_CHECKERR:
    if (errp) {
        writeResponse(Response(&req, errp));
        errp.clear();
        return true;
    }

    ResultOptions opts = ResultOptions(req.payload["Options"]);

    // There are some sanity checking operations we should perform.
    // Because we wrap the handle, and the handle cannot return any responses.

    // Continuous must be used with IterWait
    if (needs_ds && dstype == Dataset::DSTYPE_SEEDED) {
        if (opts.iterwait == false &&
                ((DatasetSeeded*)ds)->getSpec().continuous == true) {
            writeResponse(Response(&req, Error(Error::SUBSYSf_SDKD,
                                               Error::SDKD_EINVAL,
                                               "Continuous must be used with IterWait")));
            return true;
        }
    }

    switch (req.command) {
    case Command::MC_DS_DELETE:
    case Command::MC_DS_TOUCH:
        h.dsKeyop(req.command, *ds, rs, opts);
        break;

    case Command::MC_DS_MUTATE_ADD:
    case Command::MC_DS_MUTATE_REPLACE:
    case Command::MC_DS_MUTATE_APPEND:
    case Command::MC_DS_MUTATE_PREPEND:
    case Command::MC_DS_MUTATE_SET:
        h.dsMutate(req.command, *ds, rs, opts);
        break;

    case Command::MC_DS_GET:
        h.dsGet(req.command, *ds, rs, opts);
        break;

    // case Command::MC_DS_GETREPLICA:
    //     h.dsGetReplica(req.command, *ds, rs, opts);
    //     break;

    // case Command::MC_DS_ENDURE:
    //     h.dsEndure(req.command, *ds, rs, opts);
    //     break;

    // case Command::MC_DS_OBSERVE:
    //     h.dsObserve(req.command, *ds, rs, opts);
    //     break;

    case Command::MC_DS_SD_RUN: {
        h.dsSDSinglePath(req.command, *ds, rs, opts);
        break;
    }

    case Command::MC_DS_SD_LOAD: {
        SDLoader sdLoader = SDLoader(cur_handle);
        sdLoader.populate(*ds);
        break;
    }

    case Command::CB_VIEW_LOAD:
    {
        ViewLoader vl = ViewLoader(cur_handle);
        vl.populateViewData(req.command, *ds, rs, opts, req);
        break;
    }

    case Command::CB_VIEW_QUERY:
    {
        ViewExecutor ve = ViewExecutor(cur_handle);
        ve.executeView(req.command, rs, opts, req);
        break;
    }

    case Command::CB_N1QL_LOAD:
    {
        N1QLLoader nl = N1QLLoader(cur_handle);
        nl.populate(*ds);
        break;
    }

    case Command::CB_N1QL_QUERY:
    {
        N1QLQueryExecutor qe = N1QLQueryExecutor(cur_handle);
        qe.execute(req.command, rs, opts, req);
        break;
    }

    case Command::CB_FTS_LOAD:
    {
        FTSLoader fl = FTSLoader(cur_handle);
        fl.populate(*ds);
        break;
    }

    case Command::CB_FTS_QUERY:
    {
        FTSQueryExecutor fe = FTSQueryExecutor(cur_handle);
        fe.execute(rs, opts, req);
        break;
    }

    case Command::CB_AS_LOAD:
    {
        CBASLoader cbas_l = CBASLoader(cur_handle);
        cbas_l.populate(*ds);
        break;
    }

    case Command::CB_AS_QUERY:
    {
        CBASQueryExecutor cbas_e = CBASQueryExecutor(cur_handle);
        cbas_e.execute(rs, opts, req);
        break;
    }

    default:
        log_warn("Command '%s' not implemented",
                 req.command.cmdstr.c_str());
        writeResponse(Response(&req, Error(Error::SUBSYSf_SDKD,
                                            Error::SDKD_ENOIMPL)));
        return true;
    }

    if (rs.getError()) {
        writeResponse(Response(&req, rs.getError()));
    } else {
        Response resp = Response(&req);
        Json::Value root;
        rs.resultsJson(&root);
        resp.setResponseData(root);
        writeResponse(resp);
    }

    return true;
}

bool
WorkerDispatch::selectLoop()
{
    fd_set rfd, wfd, efd;
    setupFdSets(&rfd, &wfd, &efd);
    int selv = select((int)sockfd + 1, &rfd, &wfd, &efd, NULL);
    if (selv == SOCKET_ERROR) {
        int errno_save = sdkd_socket_errno();
        if (errno_save != EINTR) {
            return false;
        }
        return true;
    }

    if (selv == 0) {
        fprintf(stderr, "Select() returned 0, but FDs were passed\n");
        return false;
    }
    if (FD_ISSET(sockfd, &efd)) {
        fprintf(stderr, "Socket errored. \n");
        return false;
    }
    if (FD_ISSET(sockfd, &wfd)) {
        if (flushBuffer() == -1) {
            return false;
        }
    }
    if (FD_ISSET(sockfd, &rfd)) {
        Request *reqp;
        int rv;
        rv = readRequest(&reqp);
        if (rv == -1) {
            return false;
        }
        if (rv == 0) {
            return true;
        }
        bool ret = processRequest(*reqp);
        delete reqp;
        return ret;
    }
    return true;
}

// Worker does some stuff here.. dispatching commands to its own handle
// instance.
// This is actually two phases because we first need to wait and receive the
// initial control message..
void
WorkerDispatch::run() {

    Request req;
    Error errp;
    HandleOptions hopts;
    int rv = sdkd_make_socket_nonblocking(sockfd, 1);
    if (rv == -1) {
        perror("Couldn't make socket blocking\n");
    }

    while (selectLoop()) {
        ;
    }

    flushBufferBlock();

    if (cur_hid) {
        /* ... */
        parent->unregisterWDHandle(cur_hid);
        cur_hid = 0;
    }

    hmutex->lock();
    if (cur_handle) {
        cur_handle->cancelCurrent();
        delete cur_handle;
        cur_handle = NULL;
    }
    hmutex->unlock();
}

// We need significant indirection because of shared data access from multiple
// threads, and the fact that we want to make sure the handle itself is never
// destroyed from anything but the thread it was created in. (Some components
// may not be thread safe)
void
WorkerDispatch::cancelCurrentHandle()
{
    hmutex->lock();
    if (cur_handle) {
        cur_handle->cancelCurrent();
    }
    hmutex->unlock();
}

}
