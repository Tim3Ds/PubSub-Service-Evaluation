#ifndef STATS_COLLECTOR_HPP
#define STATS_COLLECTOR_HPP

#include <vector>
#include <algorithm>
#include <cmath>
#include <chrono>
#include "json.hpp"

using json = nlohmann::json;

class MessageStats {
public:
    MessageStats() : sent_count(0), received_count(0), processed_count(0), failed_count(0) {}
    
    void record_message(bool success, double timing_ms = 0) {
        sent_count++;
        if (success) {
            received_count++;
            processed_count++;
            if (timing_ms >= 0) {
                message_timings.push_back(timing_ms);
            }
        } else {
            failed_count++;
        }
    }
    
    void set_duration(long long start_ms, long long end_ms) {
        start_time = start_ms;
        end_time = end_ms;
    }
    
    void set_metadata(const json& meta) {
        metadata = meta;
    }
    
    void add_metadata(const std::string& key, const json& value) {
        metadata[key] = value;
    }
    
    double get_duration_ms() const {
        if (start_time > 0 && end_time > 0) {
            return end_time - start_time;
        }
        return 0.0;
    }
    
    json get_stats() const {
        json stats = metadata;
        double duration = get_duration_ms();
        
        stats["total_sent"] = sent_count;
        stats["total_received"] = received_count;
        stats["total_processed"] = processed_count;
        stats["total_failed"] = failed_count;
        stats["duration_ms"] = duration;
        stats["messages_per_ms"] = duration > 0 ? (double)processed_count / duration : 0.0;
        stats["failed_per_ms"] = duration > 0 ? (double)failed_count / duration : 0.0;
        
        if (!message_timings.empty()) {
            json timing_stats;
            auto min_val = *std::min_element(message_timings.begin(), message_timings.end());
            auto max_val = *std::max_element(message_timings.begin(), message_timings.end());
            
            double mean = 0;
            for (auto t : message_timings) mean += t;
            mean /= message_timings.size();
            
            timing_stats["min_ms"] = min_val;
            timing_stats["max_ms"] = max_val;
            timing_stats["mean_ms"] = mean;
            timing_stats["count"] = (int)message_timings.size();
            
            // Calculate median
            std::vector<double> sorted_timings = message_timings;
            std::sort(sorted_timings.begin(), sorted_timings.end());
            if (sorted_timings.size() % 2 == 0) {
                timing_stats["median_ms"] = (sorted_timings[sorted_timings.size()/2 - 1] + sorted_timings[sorted_timings.size()/2]) / 2.0;
            } else {
                timing_stats["median_ms"] = sorted_timings[sorted_timings.size()/2];
            }
            
            // Calculate standard deviation
            if (message_timings.size() > 1) {
                double variance = 0;
                for (auto t : message_timings) {
                    variance += (t - mean) * (t - mean);
                }
                variance /= message_timings.size();
                timing_stats["stdev_ms"] = std::sqrt(variance);
            }
            
            stats["message_timing_stats"] = timing_stats;
        }
        
        return stats;
    }
    
    int sent_count;
    int received_count;
    int processed_count;
    int failed_count;
    
private:
    std::vector<double> message_timings;
    long long start_time = 0;
    long long end_time = 0;
    json metadata = json::object();
};

#endif // STATS_COLLECTOR_HPP
