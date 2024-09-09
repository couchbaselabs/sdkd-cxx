#include "sdkd_internal.h"

namespace CBSdkd
{

bool
SDDataset::verify_spec(void)
{
    if (!spec.count) {
        this->err.setCode(Error::SDKD_EINVAL | Error::SUBSYSf_SDKD);
        this->err.errstr = "Must have count";
        return false;
    }

    return true;
}

SDDataset::SDDataset(const Json::Value& jspec, bool load)
  : Dataset(Dataset::DSTYPE_SD)
{
    struct SDDatasetSpecification* spec = &this->spec;
    spec->load = load;
    if (load) {
        const Json::Value& doc = jspec[CBSDKD_MSGFLD_SD_SCHEMA].asString();
        spec->doc = doc;

    } else {
        spec->command = jspec[CBSDKD_MSGFLD_SD_COMMAND].asString();
        spec->path = jspec[CBSDKD_MSGFLD_SD_PATH].asString();
        spec->value = jspec[CBSDKD_MSGFLD_SD_VALUE].asString();
    }

    spec->count = jspec[CBSDKD_MSGFLD_SD_COUNT].asUInt();
    spec->continuous = jspec[CBSDKD_MSGFLD_DSREQ_CONTINUOUS].asBool();
    verify_spec();
}

SDDataset::SDDataset(const struct SDDatasetSpecification& spec)
  : Dataset(Dataset::DSTYPE_SD)
{
    this->spec = spec;
}

SDDatasetIterator*
SDDataset::getIter() const
{
    return new SDDatasetIterator(&this->spec);
}

unsigned int
SDDataset::getCount() const
{
    return spec.count;
}

SDDatasetIterator::SDDatasetIterator(const struct SDDatasetSpecification* spec)
{
    this->spec = spec;
}

void
SDDatasetIterator::advance()
{
    curidx++;
    if (spec->continuous && curidx >= spec->count) {
        curidx = 0;
    }
    init_data(curidx);
}

void
SDDatasetIterator::init_data(int idx)
{
    if (spec->load) {
        Json::Value doc = spec->doc;
        this->curv = Json::FastWriter().write(doc);
        std::string newcurv;
        for (std::string::iterator it = this->curv.begin(); it != this->curv.end(); ++it) {
            if (*it != '\\' && it != this->curv.begin() && it != this->curv.end() - 2 && it != this->curv.end() - 1 &&
                it != this->curv.end()) {
                newcurv += *it;
            }
        }
        this->curv = newcurv;
    } else {
        this->curp = spec->path;
        this->curv = spec->value;
        this->curc = spec->command;
    }
    this->curk = std::to_string(idx);
}

bool
SDDatasetIterator::done()
{

    if (this->spec->continuous) {
        return false;
    }

    if (this->curidx >= this->spec->count) {
        return true;
    }
    return false;
}
} /* namespace CBSdkd */
