#pragma once

#include <string>

namespace services::constants {
    typedef std::string_view metadata;
    // defines the size of a row or column in the database.
    constexpr metadata size_md = "m_size";
    // defines the round number.
    constexpr metadata round_md = "round";
    // defines the epoch number.
    constexpr metadata epoch_md = "epoch";
    // defines the credentials of the worker.
    constexpr metadata credentials_md = "credentials";

    // defines the max message size a server expects: 5mb.
    constexpr int max_message_size = 1024 * 1024 * 5;
    // constexpr int max_message_size = 1024 * 1024 * 256;
}