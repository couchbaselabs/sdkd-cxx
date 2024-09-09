/*
 * Dataset.cpp
 *
 *  Created on: May 11, 2012
 *      Author: mnunberg
 */

#include "sdkd_internal.h"

namespace CBSdkd
{

Dataset::Dataset(Type t)
  : err((Error::Code)0)
  , type(t)
{
}

bool
Dataset::isValid()
{
    if (this->err.code == Error::SUCCESS) {
        return true;
    }
    return false;
}

// Here a dataset may either contain actual data, or a reference to a pre-defined
// dataset.. We only return the type. The caller should determine
// The proper constructor

Dataset::Type
Dataset::determineType(const Request& req, std::string* refid)
{
    // json is CommandData
    Type ret;

    if (!req.payload[CBSDKD_MSGFLD_DSREQ_DSTYPE]) {
        return DSTYPE_INVALID;
    }

    if (!req.payload[CBSDKD_MSGFLD_DSREQ_DS]) {
        return DSTYPE_INVALID;
    }

    std::string typestr = req.payload[CBSDKD_MSGFLD_DSREQ_DSTYPE].asString();
    const Json::Value& dsdata = req.payload[CBSDKD_MSGFLD_DSREQ_DS];

#define XX(c)                                                                                                                              \
    if (typestr == #c) {                                                                                                                   \
        ret = c;                                                                                                                           \
        goto GT_DONE;                                                                                                                      \
    }
    CBSDKD_DATASET_XTYPE(XX)
#undef XX

    return DSTYPE_INVALID;

GT_DONE:
    if (ret == DSTYPE_REFERENCE) {
        if (!dsdata[CBSDKD_MSGFLD_DS_ID]) {
            return DSTYPE_INVALID;
        } else {
            if (refid)
                *refid = dsdata[CBSDKD_MSGFLD_DS_ID].asString();
        }
    } else {
        if (refid)
            *refid = "";
    }
    return ret;
}

Dataset*
Dataset::fromType(Type t, const Request& req)
{
    Dataset* ret;
    if (t == DSTYPE_INLINE) {
        ret = new DatasetInline(req.payload[CBSDKD_MSGFLD_DSREQ_DS]);
    } else if (t == DSTYPE_SEEDED) {
        assert(!req.payload[CBSDKD_MSGFLD_DSREQ_DS].empty());
        ret = new DatasetSeeded(req.payload[CBSDKD_MSGFLD_DSREQ_DS]);
    } else if (t == DSTYPE_N1QL) {
        assert(!req.payload[CBSDKD_MSGFLD_DSREQ_DS].empty());
        ret = new N1QLDataset(req.payload[CBSDKD_MSGFLD_DSREQ_DS]);
    } else if (t == DSTYPE_SD) {
        assert(!req.payload[CBSDKD_MSGFLD_DSREQ_DS].empty());
        if (req.command == Command::MC_DS_SD_LOAD) {
            ret = new SDDataset(req.payload[CBSDKD_MSGFLD_DSREQ_DS], true);
        } else {
            ret = new SDDataset(req.payload[CBSDKD_MSGFLD_DSREQ_DS], false);
        }
    } else if (t == DSTYPE_FTS) {
        assert(!req.payload[CBSDKD_MSGFLD_DSREQ_DS].empty());
        ret = new FTSDataset(req.payload[CBSDKD_MSGFLD_DSREQ_DS]);
    } else if (t == DSTYPE_CBAS) {
        assert(!req.payload[CBSDKD_MSGFLD_DSREQ_DS].empty());
        ret = new CBASDataset(req.payload[CBSDKD_MSGFLD_DSREQ_DS]);
    } else {
        ret = NULL;
    }
    return ret;
}

const std::string
DatasetIterator::key() const
{
    return curk;
}

const std::string
DatasetIterator::value() const
{
    return curv;
}

const std::string
DatasetIterator::path() const
{
    return curp;
}

const std::string
DatasetIterator::command() const
{
    return curc;
}

void
DatasetIterator::start()
{
    curidx = 0;
    init_data(curidx);
}

void
DatasetIterator::advance()
{
    curidx++;
    init_data(curidx);
}

DatasetInline::DatasetInline(const Json::Value& json)
  : Dataset(Dataset::DSTYPE_INLINE)
{
    const Json::Value& dsitems = json[CBSDKD_MSGFLD_DSINLINE_ITEMS];

    if (dsitems.empty()) {
        this->err = Error(Error::SDKD_EINVAL, "Expected 'Items' but couldn't find any");
        return;
    }
    this->items = dsitems;
}

DatasetIterator*
DatasetInline::getIter() const
{
    return new DatasetInlineIterator(&this->items);
}

unsigned int
DatasetInline::getCount() const
{
    return this->items.size();
}

DatasetInlineIterator::DatasetInlineIterator(const Json::Value* items)
{

    this->items = items;
    this->curidx = 0;
}

void
DatasetInlineIterator::init_data(int idx)
{
    Json::Value pair = (*this->items)[idx];
    if (pair.isArray()) {
        this->curk = pair[0].asString();
        this->curv = pair[1].asString();
    } else {
        this->curk = pair.asString();
    }
}

bool
DatasetInlineIterator::done()
{
    if (this->curidx >= this->items->size()) {
        return true;
    }
    return false;
}

bool
DatasetSeeded::verify_spec(void)
{
    if (!spec.count) {
        this->err.setCode(Error::SDKD_EINVAL | Error::SUBSYSf_SDKD);
        this->err.errstr = "Must have count";
        return false;
    }

    if (spec.kseed.size() == 0 || spec.vseed.size() == 0) {
        this->err.setCode(Error::SDKD_EINVAL | Error::SUBSYSf_SDKD);
        this->err.errstr = "KSeed and VSeed must not be empty";
        return false;
    }
    return true;
}

DatasetSeeded::DatasetSeeded(const Json::Value& jspec)
  : Dataset(Dataset::DSTYPE_SEEDED)
{
    struct DatasetSeedSpecification* spec = &this->spec;

    spec->kseed = jspec[CBSDKD_MSGFLD_DSSEED_KSEED].asString();
    spec->vseed = jspec[CBSDKD_MSGFLD_DSSEED_VSEED].asString();
    spec->ksize = jspec[CBSDKD_MSGFLD_DSSEED_KSIZE].asUInt();
    spec->vsize = jspec[CBSDKD_MSGFLD_DSSEED_VSIZE].asUInt();

    spec->repeat = jspec[CBSDKD_MSGFLD_DSSEED_REPEAT].asString();
    spec->count = jspec[CBSDKD_MSGFLD_DSSEED_COUNT].asUInt();
    spec->continuous = jspec[CBSDKD_MSGFLD_DSREQ_CONTINUOUS].asBool();
    verify_spec();
}

DatasetSeeded::DatasetSeeded(const struct DatasetSeedSpecification& spec)
  : Dataset(Dataset::DSTYPE_SEEDED)
{
    //    memcpy(&this->spec, spec, sizeof(this->spec));
    this->spec = spec;
}

DatasetIterator*
DatasetSeeded::getIter() const
{
    return new DatasetSeededIterator(&this->spec);
}

unsigned int
DatasetSeeded::getCount() const
{
    return spec.count;
}

DatasetSeededIterator::DatasetSeededIterator(const struct DatasetSeedSpecification* spec)
{
    this->spec = spec;
}

static const std::string
_fill_repeat(const std::string base, unsigned int limit, const std::string repeat, unsigned int idx)
{
    char idxbuf[32] = { 0 };
    sprintf(idxbuf, "%u", idx);
    const std::string repeat_ext = repeat + idxbuf;
    std::string result = base + repeat_ext;

    result.reserve(limit + repeat_ext.length());
    while (result.size() < limit - 1) {
        result += repeat_ext;
    }

    return result;
}

void
DatasetSeededIterator::advance()
{
    curidx++;
    if (spec->continuous && curidx >= spec->count) {
        curidx = 0;
    }
    init_data(curidx);
}

void
DatasetSeededIterator::init_data(int idx)
{

    this->curk = _fill_repeat(this->spec->kseed, this->spec->ksize, this->spec->repeat, idx);
    this->curv = _fill_repeat(this->spec->vseed, this->spec->vsize, this->spec->repeat, idx);
}

bool
DatasetSeededIterator::done()
{

    if (this->spec->continuous) {
        // Continuous always returns True
        return false;
    }

    if (this->curidx >= this->spec->count) {
        return true;
    }
    return false;
}

} /* namespace CBSdkd */
