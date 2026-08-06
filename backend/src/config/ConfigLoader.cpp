#include "ConfigLoader.hpp"
#include "algorithms/RateLimiterFactory.hpp"
#include <nlohmann/json.hpp>
#include <fstream>
#include <stdexcept>

using json = nlohmann::json;

namespace ratelimiter {

namespace {
RateLimitConfig parseLimit(const json& j) {
    RateLimitConfig cfg;
    cfg.requests = j.at("requests").get<int64_t>();
    cfg.window = std::chrono::milliseconds(j.at("window_ms").get<int64_t>());
    cfg.refill_rate = j.at("refill_rate").get<double>();
    cfg.burst_capacity = j.at("burst_capacity").get<int64_t>();
    return cfg;
}
}

ConfigLoader::ConfigLoader(std::string path) : path_(std::move(path)) {}

AppConfig ConfigLoader::parseFile(const std::string& path) const {
    std::ifstream file(path);
    if (!file.is_open()) {
        throw std::runtime_error("ConfigLoader: cannot open config file: " + path);
    }

    json j;
    try {
        file >> j;
    } catch (const json::parse_error& e) {
        throw std::runtime_error(std::string("ConfigLoader: malformed JSON: ") + e.what());
    }

    AppConfig cfg = AppConfig::withDefaults();
    try {
        cfg.algorithm = RateLimiterFactory::fromString(j.at("algorithm").get<std::string>());
        cfg.server_port = j.value("server_port", 8080);

        cfg.plan_limits[PlanTier::Free]       = parseLimit(j.at("plans").at("free"));
        cfg.plan_limits[PlanTier::Premium]    = parseLimit(j.at("plans").at("premium"));
        cfg.plan_limits[PlanTier::Enterprise] = parseLimit(j.at("plans").at("enterprise"));

        cfg.global_limit = parseLimit(j.at("global_limit"));
        cfg.per_ip_limit = parseLimit(j.at("per_ip_limit"));

        cfg.redis_host = j.at("redis").value("host", "localhost");
        cfg.redis_port = j.at("redis").value("port", 6379);
        cfg.postgres_conn_string = j.at("postgres").at("conn_string").get<std::string>();
        cfg.jwt_secret = j.at("jwt_secret").get<std::string>();
    } catch (const json::exception& e) {
        throw std::runtime_error(std::string("ConfigLoader: missing/invalid field: ") + e.what());
    }

    return cfg;
}

void ConfigLoader::load() {
    AppConfig parsed = parseFile(path_);
    std::unique_lock lock(mutex_);
    config_ = std::move(parsed);
}

void ConfigLoader::reload() {
    AppConfig parsed = parseFile(path_);
    std::unique_lock lock(mutex_);
    config_ = std::move(parsed);
}

AppConfig ConfigLoader::getSnapshot() const {
    std::shared_lock lock(mutex_);
    return config_; 
}

void ConfigLoader::persist(const AppConfig& config) const {
    json j;
    j["algorithm"] = "token_bucket"; 
    j["server_port"] = config.server_port;
    std::ofstream file(path_);
    file << j.dump(2);
}

} 