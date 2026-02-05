/**
 * @file test_data_loader.cpp
 * @brief Test Data Loader - Implementation for loading test_data.json files.
 * 
 * This module provides a unified interface for loading test data across all
 * C++ sender implementations, eliminating code duplication and providing consistent
 * path resolution.
 */
#include "test_data_loader.hpp"
#include "json.hpp"
#include <sstream>
#include <stdexcept>

using json = nlohmann::json;

namespace test_data_loader {
    
    std::filesystem::path getDefaultTestDataPath() {
        // List of potential locations to search
        std::filesystem::path search_paths[] = {
            std::filesystem::current_path(),                    // Current working directory
            std::filesystem::current_path().parent_path(),      // Parent directory (repo root)
            std::filesystem::path("/home/tim/repos")            // Absolute path
        };
        
        for (const auto& base_path : search_paths) {
            std::filesystem::path test_data_path = base_path / "test_data.json";
            if (std::filesystem::exists(test_data_path)) {
                return test_data_path;
            }
        }
        
        // If not found in search paths, return default location in /home/tim/repos
        return std::filesystem::path("/home/tim/repos") / "test_data.json";
    }
    
    std::filesystem::path resolveTestDataPath(const std::string& data_path) {
        std::filesystem::path path;
        
        if (!data_path.empty()) {
            // Use provided path
            path = std::filesystem::path(data_path);
            if (!path.is_absolute()) {
                // If relative, make it relative to current working directory
                path = std::filesystem::current_path() / path;
            }
        } else {
            // Use default path resolution
            path = getDefaultTestDataPath();
        }
        
        // Verify the file exists
        if (!std::filesystem::exists(path)) {
            std::ostringstream oss;
            oss << "test_data.json not found at: " << path.string();
            throw std::runtime_error(oss.str());
        }
        
        // Check if it's a file
        if (!std::filesystem::is_regular_file(path)) {
            std::ostringstream oss;
            oss << "Path is not a file: " << path.string();
            throw std::runtime_error(oss.str());
        }
        
        return path;
    }
    
    std::vector<json> loadTestData(const std::string& data_path) {
        try {
            std::filesystem::path resolved_path = resolveTestDataPath(data_path);
            
            std::ifstream file(resolved_path);
            if (!file.is_open()) {
                std::ostringstream oss;
                oss << "Failed to open test data file: " << resolved_path.string();
                throw std::runtime_error(oss.str());
            }
            
            json test_data;
            file >> test_data;
            file.close();
            
            return test_data.get<std::vector<json>>();
            
        } catch (const json::parse_error& e) {
            std::ostringstream oss;
            oss << "Invalid JSON in test data file: " << e.what();
            throw std::runtime_error(oss.str());
        } catch (const std::runtime_error&) {
            // Re-throw runtime errors from resolveTestDataPath
            throw;
        } catch (const std::exception& e) {
            std::ostringstream oss;
            oss << "Failed to load test data: " << e.what();
            throw std::runtime_error(oss.str());
        }
    }
    
    size_t getTestDataCount(const std::string& data_path) {
        std::filesystem::path resolved_path = resolveTestDataPath(data_path);
        
        std::ifstream file(resolved_path);
        if (!file.is_open()) {
            std::ostringstream oss;
            oss << "Failed to open test data file: " << resolved_path.string();
            throw std::runtime_error(oss.str());
        }
        
        json test_data;
        file >> test_data;
        file.close();
        
        return test_data.size();
    }
    
    std::pair<bool, std::vector<std::string>> validateTestData(const std::vector<json>& test_data) {
        std::vector<std::string> issues;
        
        for (size_t i = 0; i < test_data.size(); ++i) {
            const auto& msg = test_data[i];
            
            if (!msg.is_object()) {
                issues.push_back("Message " + std::to_string(i) + " is not a dictionary");
                continue;
            }
            
            // Check for common required fields
            if (!msg.contains("message_id")) {
                issues.push_back("Message " + std::to_string(i) + " is missing 'message_id' field");
            }
            
            if (!msg.contains("target")) {
                issues.push_back("Message " + std::to_string(i) + " is missing 'target' field");
            }
            
            if (!msg.contains("payload")) {
                issues.push_back("Message " + std::to_string(i) + " is missing 'payload' field");
            }
        }
        
        return {issues.empty(), issues};
    }
    
    void printUsage() {
        std::cout << "Test Data Loader - Simple Test" << std::endl;
        std::cout << "==============================" << std::endl;
        std::cout << std::endl;
        std::cout << "Usage:" << std::endl;
        std::cout << "  1. Include test_data_loader.hpp in your code" << std::endl;
        std::cout << "  2. Call loadTestData() to load test data" << std::endl;
        std::cout << std::endl;
        std::cout << "Example:" << std::endl;
        std::cout << "  #include \"test_data_loader.hpp\"" << std::endl;
        std::cout << "  auto test_data = test_data_loader::loadTestData();" << std::endl;
        std::cout << std::endl;
        std::cout << "Or with custom path:" << std::endl;
        std::cout << "  auto test_data = test_data_loader::loadTestData(\"/path/to/data.json\");" << std::endl;
    }
    
} // namespace test_data_loader

// Simple standalone test when compiled directly
#ifdef TEST_DATA_LOADER_STANDALONE

int main() {
    test_data_loader::printUsage();
    std::cout << std::endl;
    
    try {
        // Try to load test data
        auto test_data = test_data_loader::loadTestData();
        std::cout << "Successfully loaded " << test_data.size() << " messages" << std::endl;
        
        // Validate structure
        auto [is_valid, issues] = test_data_loader::validateTestData(test_data);
        if (is_valid) {
            std::cout << "Test data structure is valid" << std::endl;
        } else {
            std::cout << "Validation issues found:" << std::endl;
            for (const auto& issue : issues) {
                std::cout << "  - " << issue << std::endl;
            }
        }
        
        // Show sample message
        if (!test_data.empty()) {
            std::cout << "\nSample message (first item):" << std::endl;
            std::cout << test_data[0].dump(2) << std::endl;
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

#endif // TEST_DATA_LOADER_STANDALONE

