/*
 * N1QLDataset.cpp
 *
 *  Created on: Dec 15, 2015
 *      Author: subalakr
 */

#include "sdkd_internal.h"

namespace CBSdkd
{

bool
N1QLDataset::verify_spec(void)
{
    if (!spec.count) {
        this->err.setCode(Error::SDKD_EINVAL | Error::SUBSYSf_SDKD);
        this->err.errstr = "Must have count";
        return false;
    }

    if (spec.params.size() == 0 || spec.paramValues.size() == 0) {
        this->err.setCode(Error::SDKD_EINVAL | Error::SUBSYSf_SDKD);
        this->err.errstr = "params and values must not be empty";
        return false;
    }
    return true;
}

N1QLDataset::N1QLDataset(const Json::Value& jspec)
  : Dataset(Dataset::DSTYPE_N1QL)
{
    struct N1QLDatasetSpecification* spec = &this->spec;

    split(jspec[CBSDKD_MSGFLD_NQ_PARAM].asString(), ',', spec->params);
    split(jspec[CBSDKD_MSGFLD_NQ_PARAMVALUES].asString(), ',', spec->paramValues);

    spec->count = jspec[CBSDKD_MSGFLD_NQ_COUNT].asUInt();
    spec->continuous = jspec[CBSDKD_MSGFLD_DSREQ_CONTINUOUS].asBool();
    verify_spec();
}

N1QLDataset::N1QLDataset(const struct N1QLDatasetSpecification& spec)
  : Dataset(Dataset::DSTYPE_N1QL)
{
    this->spec = spec;
}

void
N1QLDataset::split(const std::string& s, char delim, std::vector<std::string>& elems)
{
    std::stringstream ss(s);
    std::string item;

    while (std::getline(ss, item, delim)) {
        if (!item.empty()) {
            elems.push_back(item);
        }
    }
}

N1QLDatasetIterator*
N1QLDataset::getIter() const
{
    return new N1QLDatasetIterator(&this->spec);
}

unsigned int
N1QLDataset::getCount() const
{
    return spec.count;
}

N1QLDatasetIterator::N1QLDatasetIterator(const struct N1QLDatasetSpecification* spec)
{
    this->spec = spec;
}

void
N1QLDatasetIterator::advance()
{
    curidx++;
    if (spec->continuous && curidx >= spec->count) {
        curidx = 0;
    }
    init_data(curidx);
}

void
N1QLDatasetIterator::init_data(int idx)
{
    std::vector<std::string> params = spec->params;
    std::vector<std::string> paramValues = spec->paramValues;
    std::vector<std::string>::iterator pit = params.begin();
    std::vector<std::string>::iterator vit = paramValues.begin();
    Json::Value doc;

    doc["id"] = std::to_string(idx);
    for (; pit < params.end(); pit++, vit++) {
        doc[*pit] = *vit;
    }

    this->curv = Json::FastWriter().write(doc);
    this->curk = doc["id"].asString();
}

bool
N1QLDatasetIterator::done()
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
} // namespace CBSdkd
