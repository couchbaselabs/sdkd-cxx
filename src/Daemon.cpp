#include "sdkd_internal.h"
#include "core/meta/version.hxx"

#include <iostream>
#include <algorithm>

using namespace CBSdkd;
using namespace std;

Daemon::Daemon(const DaemonOptions& userOptions)
: myOptions(userOptions),
  infoFp(NULL)

{
    if (myOptions.portFile == NULL && myOptions.portNumber == 0) {
        cerr << "Must specify a port file or port number" << endl;
        exit(1);
    }

    if (myOptions.portFile) {
        infoFp = fopen(myOptions.portFile, "w");
        if (!infoFp) {
            perror(myOptions.portFile);
            exit(0);
        }
    }

#ifndef _WIN32
    signal(SIGPIPE, SIG_IGN);
#endif


    initDebugSettings();

    create_logger(myOptions.lcblogFile);

    // SDKD_INIT_VIEWS();
    SDKD_INIT_WORKER_GLOBALS();

    prepareAddress();

    sdkd_init_timer();
    sdkd_set_ttl(myOptions.initialTTL);

}

Daemon::~Daemon()
{
    if (myOptions.portFile != NULL) {
        remove(myOptions.portFile);
    }
    destroy_logger();
}

void
Daemon::prepareAddress()
{
    memset(&listenAddr, 0, sizeof(listenAddr));
    listenAddr.sin_port = htons(myOptions.portNumber);
    listenAddr.sin_family = AF_INET;
}

void
Daemon::runServer()
{
    MainDispatch server = MainDispatch();

    if (!server.establishSocket(&listenAddr)) {
        perror("Establishing listening socket..");
        exit(1);
    }

    auto build_info = couchbase::core::meta::sdk_build_info_json();
    log_noctx_info("Listening on port %d. %s", ntohs(listenAddr.sin_port), build_info.c_str());
    writePortInfo();
    server.run();
}

void
Daemon::writePortInfo()
{
    if (!infoFp) {
        return;
    }

    fprintf(infoFp, "%d\n", ntohs(listenAddr.sin_port));
    fclose(infoFp);
    infoFp = NULL;
}

void
Daemon::initDebugSettings() {
    CBsdkd_Global_Debug_Context.cbsdkd__debugctx.level =
            (cbsdkd_loglevel_t)myOptions.debugLevel;

    CBsdkd_Global_Debug_Context.cbsdkd__debugctx.color =
            myOptions.debugColors;

    CBsdkd_Global_Debug_Context.cbsdkd__debugctx.initialized = 1;
    CBsdkd_Global_Debug_Context.cbsdkd__debugctx.out = stderr;
    CBsdkd_Global_Debug_Context.cbsdkd__debugctx.prefix = "cbskd-test";
}

void
Daemon::run()
{
    do {
        runServer();
    } while (myOptions.isPersistent);
}
