/**
 * Author: subhashni
 *
 */
#include "sdkd_internal.h"

#include <dirent.h>
#include <sys/stat.h>

namespace CBSdkd
{

extern "C" {
#ifdef _WIN32
static unsigned __stdcall new_stats_collector(void* coll)
{
    reinterpret_cast<UsageCollector*>(coll)->Loop();
    return 0;
}
#else
static void*
new_stats_collector(void* coll)
{
    ((UsageCollector*)coll)->Loop();
    return NULL;
}
#endif
}

void
UsageCollector::Start(void)
{
    thr = Thread::Create(new_stats_collector);
    thr->start(this);
}

typedef struct fdstats {
    int total;
    int files;
    int socks;
} fdstats;

static void
get_fdstats(fdstats* stats)
{
    DIR* dirp;
    struct dirent* dp;

    memset(stats, 0, sizeof(fdstats));

    if ((dirp = opendir("/proc/self/fd")) == NULL) {
        perror("Unable to open '/proc/self/fd'");
        return;
    }

    while ((dp = readdir(dirp)) != NULL) {
        stats->total++;
        switch (dp->d_type) {
            case DT_REG:
                stats->files++;
                break;
            case DT_LNK: {
                char buf[PATH_MAX] = {};
                struct stat st = {};
                snprintf(buf, sizeof(buf), "/proc/self/fd/%s", dp->d_name);
                stat(buf, &st);
                if (S_ISSOCK(st.st_mode)) {
                    stats->socks++;
                } else if (S_ISREG(st.st_mode)) {
                    stats->files++;
                }
            } break;
        }
    }
    closedir(dirp);
}

void
UsageCollector::Loop(void)
{
    struct rusage usage;
    fdstats stats;

    samplingtime = Json::Value(Json::arrayValue);
    samplingtime.append("ts");
    memusages = Json::Value(Json::arrayValue);
    memusages.append("memory");
    cputimeusages_user = Json::Value(Json::arrayValue);
    cputimeusages_user.append("cpu_user");
    cputimeusages_system = Json::Value(Json::arrayValue);
    cputimeusages_system.append("cpu_system");
    cnt_files = Json::Value(Json::arrayValue);
    cnt_files.append("files");
    cnt_connections = Json::Value(Json::arrayValue);
    cnt_connections.append("connections");
    double current_span = 0;

    while (1) {
        memset(&usage, '0', sizeof usage);
        if ((getrusage(RUSAGE_SELF, &usage) == -1)) {
            fprintf(stderr, "Unable to get usage info ! aborting");
            exit(1);
        }
        cputimeusages_user.append(usage.ru_utime.tv_sec + ((double)usage.ru_utime.tv_usec / 1000000));
        cputimeusages_system.append(usage.ru_stime.tv_sec + ((double)usage.ru_stime.tv_usec / 1000000));
        double mem = usage.ru_maxrss;
        samplingtime.append(current_span);
        memusages.append(mem);
        // get_fdstats(&stats);
        // cnt_files.append(stats.files);
        // cnt_connections.append(stats.socks);
        sleep(interval);
        current_span += interval;
    }
}

void
UsageCollector::GetResponseJson(Json::Value& res)
{
    res["ts"] = samplingtime;
    res["memory"] = memusages;
    res["cpu_user"] = cputimeusages_user;
    res["cpu_system"] = cputimeusages_system;
    res["files"] = cnt_files;
    res["connections"] = cnt_connections;
}
} // namespace CBSdkd
