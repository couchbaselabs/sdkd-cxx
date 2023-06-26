cmake_minimum_required(VERSION 3.17)
project(sdkd_cxx)

IF(NOT CMAKE_INSTALL_PREFIX)
    SET(DESTDIR inst)
    SET(CMAKE_INSTALL_PREFIX inst)
ENDIF()

SET(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(SOURCE_ROOT ${CMAKE_SOURCE_DIR})
set(CONTRIB_ROOT ${CMAKE_SOURCE_DIR}/src/contrib)
set(JSONCPP_ROOT ${CONTRIB_ROOT}/json-cpp)
set(JSONSL_ROOT ${CONTRIB_ROOT}/jsonsl)
set(CLIOPTS_ROOT ${CONTRIB_ROOT}/cliopts)

INCLUDE_DIRECTORIES(
    ${SOURCE_ROOT}
    ${JSONCPP_ROOT}/dist
    ${CLIOPTS_ROOT}
    ${CXX_ROOT})

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(COUCHBASE_CXX_CLIENT_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_TOOLS OFF CACHE BOOL "" FORCE)
set(COUCHBASE_CXX_CLIENT_BUILD_DOCS OFF CACHE BOOL "" FORCE)
add_subdirectory(${CXX_ROOT} couchbase-cxx-client)
include_directories(${PROJECT_BINARY_DIR}/couchbase-cxx-client/generated)

LIST(APPEND LD_DEPS
            couchbase_cxx_client)

SET(COMMON_SOURCES
    src/Control.cpp
    src/Collections.cpp
    src/Daemon.cpp
    src/Message.cpp
    src/Request.cpp
    src/Dataset.cpp
    src/N1QLDataset.cpp
    src/SDDataset.cpp
    src/FTSDataset.cpp
    src/CBASDataset.cpp
    src/Response.cpp
    src/Handle.cpp
    src/ResultSet.cpp
    src/IODispatch.cpp
    src/Worker.cpp
    src/SDLoader.cpp
    src/UsageCollector.cpp
    src/N1QLQueryExecutor.cpp
    src/N1QLLoader.cpp
    src/FTSLoader.cpp
    src/FTSQueryExecutor.cpp
    src/CBASLoader.cpp
    src/CBASQueryExecutor.cpp
    src/logging.cpp

    src/ViewExecutor.cpp
    src/ViewLoader.cpp

    src/sockutil.c

    ${CONTRIB_ROOT}/debug.c
    ${CLIOPTS_ROOT}/cliopts.c
    ${JSONCPP_ROOT}/dist/jsoncpp.cpp)

IF(${WIN32})
    SET(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR})
    LIST(APPEND COMMON_SOURCES
        src/win32-gettimeofday
        src/ThreadWin32.cpp)

    LIST(APPEND LD_DEPS
        ${LCB_LIBNAME}
        ws2_32.lib)

    ADD_DEFINITIONS(-D_CRT_SECURE_NO_WARNINGS)
    ADD_DEFINITIONS(-DWINVER=0x0501)
    IF(MSVC)
        SET(COMMON_FLAGS "/W3 /EHsc")
    ELSE()
        SET(COMMON_FLAGS "-gstabs -Wall")
    ENDIF()

ELSEIF(${UNIX})
    LIST(APPEND COMMON_SOURCES
        src/ThreadUnix.cpp)

    LIST(APPEND LD_DEPS
        pthread
        ${LCB_LIBNAME})

    IF(GPROF)
        LIST(APPEND LD_DEPS
            profiler)
    ENDIF(GPROF)
    #SET(COMMON_FLAGS "-Wall ${SDKD_PERF}")
    #SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
ENDIF()

#SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS}")
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS}")

ADD_EXECUTABLE(sdkd_cxx ${COMMON_SOURCES} src/Main.cpp)
ADD_EXECUTABLE(ds_test ${COMMON_SOURCES} src/DSTest.cpp)

TARGET_LINK_LIBRARIES(sdkd_cxx ${LD_DEPS})
TARGET_LINK_LIBRARIES(ds_test ${LD_DEPS})
IF(${WIN32})
    # Copy the DLL
    FOREACH(dllpath "${LCB_ROOT}/lib" "${LCB_ROOT}/bin")
        IF(EXISTS "${dllpath}/libcouchbase.dll")
            EXECUTE_PROCESS(COMMAND ${CMAKE_COMMAND} -E copy
                "${dllpath}/libcouchbase.dll"
                "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/libcouchbase.dll")
        ENDIF()
    ENDFOREACH()
ELSE()
    INSTALL(TARGETS sdkd_cxx RUNTIME DESTINATION bin)
ENDIF()
