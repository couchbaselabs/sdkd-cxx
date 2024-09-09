#include "sdkd_internal.h"
#include <cassert>

namespace CBSdkd {

ResultOptions::ResultOptions(const Json::Value& opts)
:
    full(opts[CBSDKD_MSGFLD_DSREQ_FULL].asBool()),
    preload(opts[CBSDKD_MSGFLD_DS_PRELOAD].asBool()),
    multi(opts[CBSDKD_MSGFLD_DSREQ_MULTI].asUInt()),
    expiry(opts[CBSDKD_MSGFLD_DSREQ_EXPIRY].asUInt()),
    iterwait(opts[CBSDKD_MSGFLD_DSREQ_ITERWAIT].asUInt()),
    delay_min(opts[CBSDKD_MSGFLD_DSREQ_DELAY_MIN].asUInt()),
    delay_max(opts[CBSDKD_MSGLFD_DSREQ_DELAY_MAX].asUInt()),
    delay(opts[CBSDKD_MSGFLD_DSREQ_DELAY].asUInt()),
    timeres(opts[CBSDKD_MSGFLD_DSREQ_TIMERES].asUInt()),
    persist(opts[CBSDKD_MSGFLD_DSREQ_PERSIST].asUInt()),
    replicate(opts[CBSDKD_MSGFLD_DSREQ_REPLICATE].asUInt())
{
    _determine_delay();
}

ResultOptions::ResultOptions(bool full,
                            bool preload,
                            unsigned int expiry,
                            unsigned int delay) :
    full(full),
    preload(preload),
    multi(0),
    expiry(expiry),
    iterwait(false),
    delay_min(0),
    delay_max(0),
    delay(delay),
    timeres(0),
    persist(0),
    replicate(0)
{

    _determine_delay();
}

unsigned int
ResultOptions::getDelay() const
{
    if (delay) {
        return delay;
    }
    if (delay_min == delay_max && delay_max == 0) {
        return 0;
    }
    return (delay_min + (rand() % (delay_max - delay_min)));
}

void
ResultOptions::_determine_delay() {
    if (delay) {
        delay_min = delay_max = delay;
    } else if (delay_min == delay_max) {
        delay = delay_min = delay_max;
    }
}
void
ResultSet::setRescode(std::error_code err,
                      const void *key,
                      size_t nkey,
                      bool expect_value,
                      const void *value,
                      size_t nvalue)
{
    std::string myerr{"success"};
    if (err) {
        myerr = err.message();
    }
    stats[myerr]++;
    remaining--;

    std::string strkey;

    if (nvalue) {
        strkey.assign((const char*)key, nkey);
    }

    if (options.full) {
        if (expect_value) {
            fullstats[strkey] = value ? string((const char*)value, nvalue) :
                    string();
        } else {
            fullstats[strkey] = myerr;
        }
    }

    if (!options.timeres) {
        options.timeres = 1;
    }

    struct timeval tv;
    suseconds_t msec_now = getEpochMsecs(tv);

    time_t cur_tframe;
    cur_tframe = tv.tv_sec - (tv.tv_sec % options.timeres);
    suseconds_t duration = msec_now - opstart_tmsec;

    if (!cur_wintime) {
        cur_wintime = cur_tframe;
        win_begin = cur_tframe;
        timestats.push_back(TimeWindow());
    } else if (cur_wintime < cur_tframe) {

        // In case of large outages, we might have missed several windows
        do {
            TimeWindow tmTmp;
            tmTmp.time_min = 0;
            timestats.push_back(tmTmp);

            // Add one window..
            cur_wintime += options.timeres;
        } while (cur_wintime < cur_tframe);

        // Reset the stats for the given window
        timestats.back().time_min = -1;
    }

    assert (timestats.size() > 0);
    TimeWindow& win = timestats.back();
    win.count++;
    win.time_total += duration;
    win.time_min = min(win.time_min, (unsigned int)duration);
    win.time_max = max(win.time_max, (unsigned int)duration);
    win.durations.push_back(duration);
//    assert(win.time_min < 10000000);
    win.ec[myerr]++;
}

int g_pFactor = 95;
int ResultSet::m_pFactor = 95;

void
ResultSet::resultsJson(Json::Value *in) const
{
    Json::Value
        summaries = Json::Value(Json::objectValue),
        &root = *in;

    for (std::map<std::string, int>::const_iterator iter = this->stats.begin();
            iter != this->stats.end(); iter++ ) {
        stringstream ss;
        ss << iter->first;
        summaries[ss.str()] = iter->second;
    }

    root[CBSDKD_MSGFLD_DSRES_STATS] = summaries;

    /*if (options.full) {
        Json::Value fullstats;
        for (
                std::map<std::string, FullResult>::const_iterator
                    iter = this->fullstats.begin();
                iter != this->fullstats.end();
                iter++
                )
        {
            fullstats[iter->first] = iter->second;
        }
        root[CBSDKD_MSGFLD_DSRES_FULL] = fullstats;
    }*/

    if (options.timeres) {
        Json::Value jtimes = Json::Value(Json::objectValue);
        /**
         * Timing statistics
         */
        jtimes[CBSDKD_MSGFLD_TMS_BASE] = (Json::Value::UInt64)win_begin;
        Json::Value windetails = Json::Value(Json::arrayValue);

        for (std::vector<TimeWindow>::const_iterator iter = timestats.begin();
                iter != timestats.end();
                iter++) {

            Json::Value winstat = Json::Value(Json::objectValue);
            winstat[CBSDKD_MSGFLD_TMS_COUNT] = iter->count;
            winstat[CBSDKD_MSGFLD_TMS_MIN] = iter->time_min;
            winstat[CBSDKD_MSGFLD_TMS_MAX] = iter->time_max;
            winstat[CBSDKD_MSGFLD_TMS_AVG] =
                    iter->count ? (iter->time_total / iter->count) : 0;

            Json::Value errstats = Json::Value(Json::objectValue);
            for (std::map<std::string, int>::const_iterator eiter = iter->ec.begin();
                    eiter != iter->ec.end();
                    eiter++) {
                stringstream ss;
                ss << eiter->first;
                errstats[ss.str()] = eiter->second;
            }

            winstat[CBSDKD_MSGFLD_TMS_ECS] = errstats;
            winstat[CBSDKD_MSGFLD_TMS_PERCENTILE] = Json::Value::Int64(getPercentile(iter->durations));
            windetails.append(winstat);

        }

        jtimes[CBSDKD_MSGFLD_TMS_STEP] = options.timeres;
        jtimes[CBSDKD_MSGFLD_TMS_WINS] = windetails;

        root[CBSDKD_MSGFLD_DRES_TIMINGS] = jtimes;
    }
}
}
