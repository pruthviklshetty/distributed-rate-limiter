#include "ConfigLoader.hpp"
#include "algorithms/RateLimiterFactory.hpp"
#include <nlohmann/json.hpp>
#include <fstream>
#include <stdexcept>

using json = nlohmann::json;

namespace ratelimiter{

    namespace{
        RateLimitConfig parseLimit(const json& j){
            RateLimitConfig cfg;
            cfg.requests = j.at("requests").get<int64_t>();
            cfg.window = std::chrono::milliseconds(j.at("window_ms").get<int64_t>());
            cfg.refill_rate = j.at("refill_rate").get<double>();
            cfg.burst_capacity = j.at("burst_capacity").get<int64_t>();
            return cfg;
        }
    }
    //construtor
    ConfigLoader::ConfigLoader(std::string path) : path_(std::move(path)){}
    AppConfig ConfigLoader::(std::string& path)const{
        std::ifstream file(path);
        if(!file.is_open()){
            throw std::runtime_error("ConfigLoader: cannot open config file: "+path);
        }
        json j;
        try{
            file >> j;
        }catch(const json::parse_error& e){
            throw std::runtime_error(std::string("ConfigLoader:malformed JSON: ")+ e.what());
        }
        AppConfig cfg = AppConfig::withDefaults();
        try{
            
        }
    }

}