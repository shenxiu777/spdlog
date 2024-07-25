// Copyright(c) 2015-present, Gabi Melman & spdlog contributors.
// Distributed under the MIT License (http://opensource.org/licenses/MIT)

#pragma once

#include "dist_sink.h"
#include "fmt/bundled/core.h"
#include <spdlog/details/log_msg.h>
#include <spdlog/details/null_mutex.h>
#include <spdlog/fmt/chrono.h>
#include <spdlog/fmt/fmt.h>

#include <chrono>
#include <cstdio>
#include <deque>
#include <mutex>
#include <string>
// Multi line duplicate message removal sink.
//
// Example:
//
// #include <spdlog/sinks/dup_filter_sink.h>
// #include <spdlog/sinks/multi_dup_filter_sink.h>
// #include <spdlog/sinks/stdout_color_sinks.h>
// #include <spdlog/spdlog.h>
// int main() {
//   auto dup_filter =
//   std::make_shared<spdlog::sinks::multi_dup_filter_sink_st>(
//       10, spdlog::level::info);
//   dup_filter->add_sink(std::make_shared<spdlog::sinks::stdout_color_sink_mt>());
//   spdlog::logger l("logger", dup_filter);
//   for (int i = 0; i < 10; i++) {
//     l.info("Hello1");
//     l.info("Hello2");
//     l.info("Hello3");
//   }
//   l.info("Different Hello");
//   for (int i = 0; i < 10; i++) {
//     l.info("Hello1");
//     l.info("Hello2");
//     l.info("Hello3");
//   }
//   l.info("Different Hello");
// }
//
// Will produce:
// [2024-07-25 09:48:21.919] [logger] [info] Hello1
// [2024-07-25 09:48:21.920] [logger] [info] Hello2
// [2024-07-25 09:48:21.920] [logger] [info] Hello3
// [2024-07-25 09:48:21.920] [logger] [info] Hello1
// [2024-07-25 09:48:21.920] [logger] [info] Hello2
// [2024-07-25 09:48:21.920] [logger] [info] Hello3
// [2024-07-25 09:48:21.920] [logger] [info] Skipped 24 duplicate messages with step 3 from 2024-07-25
// 01:48:21.920561020 to 2024-07-25 01:48:21.920703053. [2024-07-25 09:48:21.920] [logger] [info] Different Hello
// [2024-07-25 09:48:21.921] [logger] [info] Hello1
// [2024-07-25 09:48:21.921] [logger] [info] Hello2
// [2024-07-25 09:48:21.921] [logger] [info] Hello3
// [2024-07-25 09:48:21.921] [logger] [info] Hello1
// [2024-07-25 09:48:21.921] [logger] [info] Hello2
// [2024-07-25 09:48:21.921] [logger] [info] Hello3
// [2024-07-25 09:48:21.921] [logger] [info] Skipped 24 duplicate messages with step 3 from 2024-07-25
// 01:48:21.921533343 to 2024-07-25 01:48:21.921595608. [2024-07-25 09:48:21.921] [logger] [info] Different Hello

namespace spdlog {
namespace sinks {
template <typename Mutex>
class multi_dup_filter_sink : public dist_sink<Mutex> {
 public:
    explicit multi_dup_filter_sink(size_t max_skip_line = 8, level::level_enum notification_level = level::info)
        : max_skip_line_{max_skip_line}, log_level_{notification_level} {}

 protected:
    size_t max_skip_line_;
    log_clock::time_point skip_start_time_;
    log_clock::time_point skip_end_time_;
    std::deque<std::string> recent_msgs_;
    std::deque<details::log_msg> recent_msgs_details_;
    size_t skip_line_count_ = 0;
    size_t current_skip_ = 0;
    level::level_enum log_level_;

    void sink_it_(const details::log_msg &msg) override {
        recent_msgs_.emplace_back(std::string(msg.payload.data(), msg.payload.size()));
        recent_msgs_details_.push_back(msg);
        if (recent_msgs_.size() > max_skip_line_ * 2) {
            recent_msgs_.pop_front();
            recent_msgs_details_.pop_front();
        }

        bool print_current_msg = false;

        if (current_skip_ != 0) {
            // status == skipping
            if (recent_msgs_[recent_msgs_.size() - 1] == recent_msgs_[recent_msgs_.size() - current_skip_ - 1]) {
                // continue skipping
                if (skip_line_count_ == 0) {
                    // this is first msg skipped
                    skip_start_time_ = msg.time;
                }
                skip_line_count_++;
            } else {
                // end skipping
                // current msg not skip, set skip_end_time_ to previous msg time
                skip_end_time_ = recent_msgs_details_[recent_msgs_details_.size() - 2].time;
                if (skip_line_count_ > 0) {
                    sink_duplicate_message_();
                }
                current_skip_ = 0;
                skip_line_count_ = 0;
                print_current_msg = true;
            }
        } else {
            // status != skipping
            find_new_multi_dup();
            print_current_msg = true;
        }
        if (print_current_msg) {
            dist_sink<Mutex>::sink_it_(msg);
        }
    }
    void sink_duplicate_message_() {
        auto buf = fmt::format("Skipped {} duplicate messages with step {} from {} to {}.", skip_line_count_,
                               current_skip_, skip_start_time_, skip_end_time_);
        auto &last_dup_msg = recent_msgs_details_[recent_msgs_details_.size() - 2];
        details::log_msg skipped_msg{last_dup_msg.time, last_dup_msg.source, last_dup_msg.logger_name, log_level_,
                                     std::string_view(buf)};
        dist_sink<Mutex>::sink_it_(skipped_msg);
    }
    void find_new_multi_dup() {
        for (size_t finding_skip = max_skip_line_; finding_skip > 0; finding_skip--) {
            if (finding_skip * 2 > recent_msgs_.size()) {
                continue;
            }
            size_t check_start_index = recent_msgs_.size() - 1;
            size_t check_stop_index = check_start_index - finding_skip;
            for (; check_start_index > check_stop_index; check_start_index--) {
                if (recent_msgs_[check_start_index] != recent_msgs_[check_start_index - finding_skip]) {
                    break;
                }
            }
            if (check_start_index == check_stop_index) {
                current_skip_ = finding_skip;  // start skipping
                break;
            }
        }
    }
};

using multi_dup_filter_sink_mt = multi_dup_filter_sink<std::mutex>;
using multi_dup_filter_sink_st = multi_dup_filter_sink<details::null_mutex>;

}  // namespace sinks
}  // namespace spdlog
