#ifndef TEST_DATA_LOADER_HPP
#define TEST_DATA_LOADER_HPP

#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <filesystem>
#include <utility>
#include <cstdint>
#include <stdexcept>
#include <sstream>

// Include json.hpp to get the proper nlohmann::json type
#include "json.hpp"

/**
 * @brief Test Data Loader - Centralized library for loading test_data.json files.
 * 
 * This module provides a unified interface for loading test data across all
 * C++ sender implementations, eliminating code duplication and providing consistent
 * path resolution.
 * 
 * Usage:
 *   #include "test_data_loader.hpp"
 *   auto test_data = test_data_loader::loadTestData("/path/to/data.json");
 */
namespace test_data_loader {
    
    /**
     * @brief Get the default path to test_data.json by searching common locations.
     * 
     * Searches in the following order:
     * 1. Current working directory
     * 2. Parent directory (repo root)
     * 3. /home/tim/repos directory (absolute path)
     * 
     * @return std::filesystem::path The path to test_data.json
     */
    std::filesystem::path getDefaultTestDataPath();
    
    /**
     * @brief Resolve the test data file path.
     * 
     * @param data_path Optional custom path. If provided, uses this path.
     *                  If empty, searches for default test_data.json.
     * @return std::filesystem::path The resolved path to test_data.json
     * @throws std::runtime_error If the file cannot be found or read.
     */
    std::filesystem::path resolveTestDataPath(const std::string& data_path = "");
    
    /**
     * @brief Load test data from a JSON file.
     * 
     * This is the main function used by senders to load test data.
     * It handles path resolution, file opening, and JSON parsing.
     * 
     * @param data_path Optional path to the test data file. If not provided,
     *                  searches for test_data.json in common locations.
     * @return std::vector<nlohmann::json> A vector of message objects loaded from the JSON file.
     * @throws std::runtime_error If the test data file cannot be found, read, or parsed.
     */
    std::vector<nlohmann::json> loadTestData(const std::string& data_path = "");
    
    /**
     * @brief Get the number of messages in the test data file without loading all data.
     * 
     * This is a lightweight function that just counts items in the JSON array
     * without fully parsing the data structure.
     * 
     * @param data_path Optional path to the test data file.
     * @return size_t Number of messages in the test data.
     * @throws std::runtime_error If the test data file cannot be found or read.
     */
    size_t getTestDataCount(const std::string& data_path = "");
    
    /**
     * @brief Validate test data structure.
     * 
     * Checks that the test data has the expected structure with required fields.
     * 
     * @param test_data The test data to validate.
     * @return std::pair<bool, std::vector<std::string>> (is_valid, list_of_issues)
     */
    std::pair<bool, std::vector<std::string>> validateTestData(const std::vector<nlohmann::json>& test_data);
    
    /**
     * @brief Print a simple test/usage message.
     */
    void printUsage();
    
} // namespace test_data_loader

#endif // TEST_DATA_LOADER_HPP

