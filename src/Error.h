/*
 * Error.h
 *
 *  Created on: May 11, 2012
 *      Author: mnunberg
 */

#ifndef ERROR_H_
#define ERROR_H_

#ifndef SDKD_INTERNAL_H_
#error "Include sdkd_internal.h first"
#endif

namespace CBSdkd {

using std::string;
using std::stringstream;

class Error {
public:
     enum Code {
        SUBSYSf_UNKNOWN      = 0x1,
        SUBSYSf_CLUSTER      = 0x2,
        SUBSYSf_CLIENT       = 0x4,
        SUBSYSf_MEMD         = 0x8,
        SUBSYSf_NETWORK      = 0x10,
        SUBSYSf_SDKD         = 0x20,
        SUBSYSf_KVOPS        = 0x40,
        SUBSYSf_VIEWS        = 0x41,
        SUBSYSf_SDK          = 0x80,
        SUBSYSf_QUERY        = 0x81,

        KVOPS_EMATCH         = 0x200,

        SDKD_EINVAL          = 0x200,
        SDKD_ENOIMPL         = 0x300,
        SDKD_ENOHANDLE       = 0x400,
        SDKD_ENODS           = 0x500,
        SDKD_ENOREQ          = 0x600,

        ERROR_GENERIC        = 0x100,

        CLIENT_ETMO          = 0x200,
        CLIENT_ESCHED        = 0x300,

        CLUSTER_EAUTH        = 0x200,
        CLUSTER_ENOENT       = 0x300,

        VIEWS_MALFORMED      = 0x200,
        VIEWS_MISMATCH       = 0x300,
        VIEWS_HTTP_ERROR     = 0x400,

        MEMD_ENOENT          = 0x200,
        MEMD_ECAS            = 0x300,
        MEMD_ESET            = 0x400,
        MEMD_EVBUCKET        = 0x500,

        RYOW_MISMATCH        = 0x300,
        SUCCESS              = 0
    };

    void setCode(int code) {
        this->code = static_cast<Error::Code>(code);
    }

    void setString(std::string errstr) {
        this->errstr = errstr;
    }

    void clear() {
        *this = 0; // trigger operator=
    }

    Error() {
        this->code = SUCCESS;
        this->errstr = "";
    }

    Error(Code code, string errstr = "") {
        this->code = code;
        this->errstr = errstr;
    }

    Error(int erri, string errstr = "") {
        this->setCode(erri);
        this->errstr = errstr;
    }

    Error(int subsys, int minor, string errstr = "") {
        this->setCode(subsys | minor);
        this->errstr = errstr;
    }


    virtual ~Error() {
        // TODO Auto-generated destructor stub
    }

    int operator= (int other) {
        this->setCode(other);
        return (int)this->code;
    }

    operator bool() const {
        if (this->code == 0) {
            return false;
        }
        return true;
    }

    std::string prettyPrint() {
        stringstream ss;
        ss << "Subsystem: " << (this->code & 0xff) << ", ";
        ss << "Code: " << (this->code >> 8) << ", ";
        ss << "Desc: " << this->errstr;
        return ss.str();
    }

    Error& operator=(Code v) {
        code = v;
        if (!v) {
            errstr = "";
        }
        return *this;
    }

    Code code;
    std::string errstr;


    static Error createInvalid(string msg)
    {
        return Error(Error::SUBSYSf_SDKD,
                     Error::SDKD_EINVAL,
                     msg);
    }
};

} /* namespace CBSdkd */
#endif /* ERROR_H_ */
