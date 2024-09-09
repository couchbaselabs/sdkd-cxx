#include "sdkd_internal.h"

#include <cstdio>
#include <filesystem>
#include <cstdlib>
#include <string>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <iostream>
#include <map>

#include "cliopts.h"

using namespace std;
using namespace CBSdkd;

DebugContext CBSdkd::CBsdkd_Global_Debug_Context;
Daemon* Daemon::MainDaemon = NULL;

class Program
{
  public:
    Program(int argc, char** argv);
    virtual ~Program()
    {
    }
    int printVersion;
    DaemonOptions userOptions;

  private:
    bool parseLegacyArgs(int argc, char** argv);
    bool parseCliOptions(int argc, char** argv);
};

bool
Program::parseLegacyArgs(int argc, char** argv)
{
    std::string kvpair;
    std::map<std::string, std::string> opt_pairs;

    for (int ii = 1; ii < argc; ii++) {
        istringstream iss(argv[ii]);

        while (getline(iss, kvpair, ',')) {
            opt_pairs[kvpair.substr(0, kvpair.find_first_of('='))] = kvpair.substr(kvpair.find_first_of('=') + 1);
        }
    }

    if (!opt_pairs["infofile"].size()) {
        return false;
    }

    userOptions.portFile = sdkd_strdup(opt_pairs["infofile"].c_str());

    if (opt_pairs["debug"].size()) {
        userOptions.debugLevel = CBSDKD_LOGLVL_DEBUG;
    }
    return true;
}

bool
Program::parseCliOptions(int argc, char** argv)
{
    cliopts_entry entries[] = {
        { 'L', "lcb log file", CLIOPTS_ARGT_STRING, &userOptions.lcblogFile, "Log file", "LCB LOG FILE" },
        { 'd', "debug", CLIOPTS_ARGT_INT, &userOptions.debugLevel, "Level (0=off, 1=most verbose, higher numbers less verbsose)", "LEVEL" },

        { 'c', "color", CLIOPTS_ARGT_NONE, &userOptions.debugColors, "Whether to use color logging" },

        { 'f',
          "portfile",
          CLIOPTS_ARGT_STRING,
          &userOptions.portFile,
          "File to write port information (if listening on random port)",
          "FILE" },

        { 'l', "listen", CLIOPTS_ARGT_UINT, &userOptions.portNumber, "Port to listen on", "PORT" },

        {
          'P',
          "persist",
          CLIOPTS_ARGT_NONE,
          &userOptions.isPersistent,
          "Keep running after GOODBYEs",
        },

        { 0, "ttl", CLIOPTS_ARGT_INT, &userOptions.initialTTL, "TTL For daemon" },

        { 'V', "version", CLIOPTS_ARGT_NONE, &printVersion, "Print versions and exit" },

        { 0 }
    };

    int last_arg;
    cliopts_parse_options(entries, argc, argv, &last_arg, NULL);
    return true;
}

Program::Program(int argc, char** argv)
  : printVersion(0)
{
    if (argc < 2) {
        fprintf(stderr, "Usage: %s [option=value...]\n", argv[0]);
        cerr << "infofile=FILE [ specify this file to exchange port information\n";
        cerr << "debug=1 [ enable debug output ]\n";
        cerr << "Extended options available (use --help)" << endl;
        exit(1);
    }

    parseLegacyArgs(argc, argv) || parseCliOptions(argc, argv);

    if (printVersion) {
        Json::Value value;
        Handle::VersionInfoJson(value);
        cout << value.toStyledString() << endl;
        exit(0);
    }
}

int
main(int argc, char** argv)
{

#ifdef _WIN32
    // Initialize winsock
    WORD wVersionRequested;
    WSADATA wsaData;
    int rv;
    wVersionRequested = MAKEWORD(2, 2);
    rv = WSAStartup(wVersionRequested, &wsaData);
    assert(rv == 0);
#else
    FILE* fp;
    fp = fopen(PID_FILE, "w");
    if (!fp) {
        auto pid_path = (std::filesystem::current_path() / "sdkd-cpp.pid").string();
        fprintf(stderr, "Cannot open pid file %s for writing, use %s", PID_FILE, pid_path.c_str());
        fp = fopen(pid_path.c_str(), "w");
    }
    fprintf(fp, "%d", (int)getpid());
    fclose(fp);
#endif

    Program program = Program(argc, argv);
    Daemon::MainDaemon = new Daemon(program.userOptions);
    Daemon::MainDaemon->run();

#ifdef _WIN32
    WSACleanup();
#endif
    return 0;
}
