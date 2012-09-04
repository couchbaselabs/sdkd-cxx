#include "Handle.h"
#include <cassert>

namespace CBSdkd {

ResultOptions::ResultOptions(const Json::Value& opts)
:
    full(opts[CBSDKD_MSGFLD_DSREQ_FULL].asBool()),
    multi(opts[CBSDKD_MSGFLD_DSREQ_MULTI].asUInt()),
    expiry(opts[CBSDKD_MSGFLD_DSREQ_EXPIRY].asUInt()),
    iterwait(opts[CBSDKD_MSGFLD_DSREQ_ITERWAIT].asBool()),
    delay_min(opts[CBSDKD_MSGFLD_DSREQ_DELAY_MIN].asUInt()),
    delay_max(opts[CBSDKD_MSGLFD_DSREQ_DELAY_MAX].asUInt()),
    delay(opts[CBSDKD_MSGFLD_DSREQ_DELAY].asUInt()),
    timeres(opts[CBSDKD_MSGFLD_DSREQ_TIMERES].asUInt())
{
    _determine_delay();
}

ResultOptions::ResultOptions(bool full, unsigned int expiry,
              unsigned int delay) :
    full(full),
    multi(0),
    expiry(expiry),
    iterwait(false),
    delay_min(0),
    delay_max(0),
    delay(delay),
    timeres(0)
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
ResultSet::setRescode(libcouchbase_error_t err,
                      const void *key,
                      size_t nkey,
                      bool expect_value,
                      const void *value,
                      size_t nvalue)
{
    int myerr = 0;
    if (err) {
        myerr = Handle::mapError(err,
                                 Error::SUBSYSf_CLIENT|Error::ERROR_GENERIC);
    }

    stats[myerr]++;
    remaining--;

    std::string strkey = string((const char*)key, nkey);

    if (options.full) {
        if (expect_value) {
            fullstats[strkey] = value ? string((const char*)value, nvalue) :
                    string();
        } else {
            fullstats[strkey] = myerr;
        }
    }

    if (!options.timeres) {
        return;
    }

    struct timeval tv;
    suseconds_t msec_now = getEpochMsecs(tv);

    time_t cur_tframe;
    cur_tframe = tv.tv_sec - (tv.tv_sec % options.timeres);
    unsigned duration = msec_now - opstart_tmsec;

    if (!cur_wintime) {
        cur_wintime = cur_tframe;
        win_begin = cur_tframe;
        timestats.push_back(TimeWindow());
    } else if (cur_wintime < cur_tframe) {
        timestats.push_back(TimeWindow());
        cur_wintime = cur_tframe;
    }

    assert (timestats.size() > 0);
    TimeWindow& win = timestats.back();
    win.count++;
    win.time_total += duration;
    win.time_min = min(win.time_min, duration);
    win.time_max = max(win.time_max, duration);
    win.ec[myerr]++;
}

void
ResultSet::resultsJson(Json::Value *in) const
{
    Json::Value
        summaries = Json::Value(Json::objectValue),
        &root = *in;

    for (std::map<int,int>::const_iterator iter = this->stats.begin();
            iter != this->stats.end(); iter++ ) {
        stringstream ss;
        ss << iter->first;
        summaries[ss.str()] = iter->second;
    }

    root[CBSDKD_MSGFLD_DSRES_STATS] = summaries;

    if (options.full) {
        Json::Value fullstats;
        for (
                std::map<std::string,FullResult>::const_iterator
                    iter = this->fullstats.begin();
                iter != this->fullstats.end();
                iter++
                )
        {
            Json::Value stat;
            stat[0] = iter->second.getStatus();
            stat[1] = iter->second.getString();
            fullstats[iter->first] = stat;
        }
        root[CBSDKD_MSGFLD_DSRES_FULL] = fullstats;
    }

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
            winstat[CBSDKD_MSGFLD_TMS_AVG]
                    = iter->time_total / iter->count;


            Json::Value errstats = Json::Value(Json::objectValue);
            for (std::map<int,int>::const_iterator eiter = iter->ec.begin();
                    eiter != iter->ec.end();
                    eiter++) {
                stringstream ss;
                ss << eiter->first;
                errstats[ss.str()] = eiter->second;
            }

            winstat[CBSDKD_MSGFLD_TMS_ECS] = errstats;
            windetails.append(winstat);

        }

        jtimes[CBSDKD_MSGFLD_TMS_STEP] = options.timeres;
        jtimes[CBSDKD_MSGFLD_TMS_WINS] = windetails;

        root[CBSDKD_MSGFLD_DRES_TIMINGS] = jtimes;
    }
}


}
