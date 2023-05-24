/*
 * IODispatch.cpp
 *
 *  Created on: May 12, 2012
 *      Author: mnunberg
 */

#include "sdkd_internal.h"
#include <iostream>

namespace CBSdkd {

extern "C" {

static struct {
    Mutex *mutex;
    Timer *timer;
    bool initialized;
} Global_Timer;

void sdkd_init_timer(void)
{
    Global_Timer.mutex = Mutex::Create();
    Global_Timer.initialized = false;
}

static void ttl_expired_func(void *unused)
{
    fprintf(stderr, "TTL Timer expired. Abort\n");
    abort();
}

void sdkd_set_ttl(unsigned seconds)
{
    Global_Timer.mutex->lock();

    if (!Global_Timer.timer) {
        Global_Timer.timer = Timer::Create(ttl_expired_func);
    }

    if (Global_Timer.initialized) {
        Global_Timer.timer->disable();
        Global_Timer.initialized = false;
    }

    if (seconds) {
        Global_Timer.timer->schedule(seconds);
        Global_Timer.initialized = 1;
    }

    Global_Timer.mutex->unlock();
}

} //extern C

IODispatch::IODispatch()
: IOProtoHandler()
{
}

IODispatch::~IODispatch()
{
}


// This function just reads data from the socket, optionally breaking it
// into new lines.
// Returns the value of 'rv' from the socket
int
IOProtoHandler::readSocket()
{
    int rv = 0;
    char buf[4096];
    while ( (rv = recv(sockfd, buf, sizeof(buf), 0) ) > 0 ) {

        for (int ii = 0; ii < rv; ii++) {
            char c = buf[ii];

            if (c != '\n') {
                inbuf += c;

            } else {
                newlines.push_back(inbuf);
                inbuf.clear();
            }
        }
    }
    return rv;
}

// Read a single line from the network, but don't return it.
// This will possibly do network I/O and return an unparsed buffer.
IOProtoHandler::IOStatus
IOProtoHandler::getRawMessage(std::string& msgbuf)
{
    if (newlines.size()) {
        msgbuf.assign(newlines.front());
        newlines.pop_front();
        if (msgbuf.size()) {
            return this->OK;
        }
    }

    while (true) {
        int rv = readSocket();

        if (newlines.size()) {
            return getRawMessage(msgbuf);
        }

        if (rv == 0) {
            closesocket(sockfd);
            log_warn("Remote closed the connection.. (without sending a CLOSE)");
            sockfd = -1;
            return this->ERR;

        } else if (rv == SOCKET_ERROR) {
            int errno_save = sdkd_socket_errno();
            if (errno_save == SDKD_SOCK_EWOULDBLOCK) {
                return this->NOTYET;

            } else if (errno_save == EINTR) {
                continue;

            } else {
                log_warn("Got other socket error; sock=%d [%d]: %s",
                         sockfd, errno_save, strerror(errno_save));

                return this->ERR;
            }
        }
    }

    return this->ERR;
}



int
IOProtoHandler::readRequest(Request **reqp)
{
    std::string reqbuf;
    IOStatus status = getRawMessage(reqbuf);

    switch (status) {
    case NOTYET:
        return 0;
    case ERR:
        return -1;
    case OK:
        assert(reqbuf.empty() == false);
        *reqp = Request::decode(reqbuf, NULL, true);
        if (!(*reqp)->isValid()) {
            writeResponse( Response(*reqp, (*reqp)->getError()) );
            delete *reqp;
            return -1;
        }
        return 1;

    default:
        fprintf(stderr, "Unrecognized code %d\n", status);
        abort();
        return -1;
    }
}

// Returns -1 on failure, 0 on success (including EWOULDBLOCK)
int
IOProtoHandler::flushBuffer()
{
    size_t remaining = outbuf.size();
    const char *sndbuf = outbuf.data();
    int fbret = 0;

    while (remaining) {
        int rv = send(sockfd, sndbuf, remaining, 0);

        if (rv > 0) {
            sndbuf += rv;
            remaining -= rv;
            continue;
        }

        if (rv == 0) {
            fbret = -1;
            break;

        } else if (rv == SOCKET_ERROR) {
            int errno_save = sdkd_socket_errno();

            if (errno_save == EINTR) {
                continue;

            } else if (errno_save == EAGAIN) {
                fbret = 0;
                break;

            } else {
                fprintf(stderr,
                        "fd=%d, remaining=%d: Got unrecognized error [%d] on send (%s)\n",
                        sockfd,
                        (int)remaining,
                        errno_save,
                        strerror(errno_save));
                fbret = -1;
                break;
            }
        }
    }

    outbuf.erase(0, outbuf.size() - remaining);
    return fbret;
}

int
IOProtoHandler::flushBufferBlock() {

    while (outbuf.size()) {
        fd_set wfd;
        FD_ZERO(&wfd);
        FD_SET(sockfd, &wfd);
        int selrv = select((int)sockfd + 1, NULL, &wfd, NULL, NULL);

        if (selrv == SOCKET_ERROR) {
            int errno_save = sdkd_socket_errno();
            if (errno_save == EINTR) {
                continue;
            }
            fprintf(stderr, "Got sleect error: %d\n", errno_save);
            return -1;
        }

        assert(FD_ISSET(sockfd, &wfd));
        if (-1 == flushBuffer()) {
            return -1;
        }
    }
    return 0;
}

void
IOProtoHandler::writeResponse(const Response& res)
{
    std::string encoded = res.encode();
    CB_LOG_TRACE("SDKD -> {}", encoded);

    if (encoded.at(encoded.size()-1) != '\n') {
        encoded += '\n';
    }

    outbuf += encoded;
}



} /* namespace CBSdkd */
