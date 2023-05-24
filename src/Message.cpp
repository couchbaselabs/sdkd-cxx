/*
 * Message.cpp
 *
 *  Created on: May 11, 2012
 *      Author: mnunberg
 */

#include "sdkd_internal.h"

namespace CBSdkd {

Command::Command(Command::Code code)
{
    this->code = code;
#define X(c) if (code == c) this->cmdstr = #c;
    CBSDKD_XCOMMAND(X)
#undef X

}

Command::Command(const string& str)
{
    this->code = INVALID_COMMAND;
    // Figure out what our command is..
#define X(c) if (str == #c) { this->code = c; }
    CBSDKD_XCOMMAND(X)
#undef X
    this->cmdstr = str;
}

bool
Message::refreshWith(const std::string& str, bool reset)
{
    Json::Value json;
    Json::Reader reader;

    if (reset) {
        reqid = 0;
        handle_id = 0;
        command.code = Command::INVALID_COMMAND;
        command.cmdstr = "";
        payload.clear();
        err = 0;
    }

    if (! reader.parse(str, json, false)) {
        err = Error(Error::SUBSYSf_SDKD, Error::SDKD_EINVAL,
                    reader.getFormattedErrorMessages());
        return false;
    }

    CB_LOG_TRACE("SDKD <- {}", str);
    this->reqid = json[CBSDKD_MSGFLD_REQID].asUInt();
    this->handle_id = json[CBSDKD_MSGFLD_HID].asUInt();
    this->command = Command(json[CBSDKD_MSGFLD_CMD].asString());
    this->payload = json;
    return true;
}

Message::Message(string& str) {
    refreshWith(str, false);
}

Message::~Message() {
    // TODO Auto-generated destructor stub
}

} /* namespace CBSdkd */
