#include <crow.h>
#include "config/ConfigLoader.hpp"
#include "storage/RedisStore.hpp"
#include "storage/PostgresLogger.hpp"
#include "services/AuthService.hpp"
#include "services/RateLimitService.hpp"
#include "controllers/RateLimitController.hpp"
#include "controllers/AuthController.hpp"
#include "controllers/AdminController.hpp"
#include "metrics/MetricsRegistry.hpp"
#include <iostream>
#include <memory>

using namespace ratelimiter;

int main(int argc, char** argv) {
    std::string config_path = argc > 1 ? argv[1] : "config/config.json";

    ConfigLoader config_loader(config_path);
    try {
        config_loader.load();
    } catch (const std::exception& e) {
        std::cerr << "FATAL: failed to load config: " << e.what() << std::endl;
        return 1;
    }
    auto config = config_loader.getSnapshot();
    std::shared_ptr<RedisStore> redis;
    try {
        redis = std::make_shared<RedisStore>(config.redis_host, config.redis_port);
        std::cout << "Connected to Redis at " << config.redis_host << ":" << config.redis_port << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "WARNING: Redis unavailable (" << e.what() << ") — running in single-node mode" << std::endl;
    }
    std::unique_ptr<PostgresLogger> logger;
    try {
        logger = std::make_unique<PostgresLogger>(config.postgres_conn_string);
        logger->start();
    } catch (const std::exception& e) {
        std::cerr << "FATAL: Postgres unavailable, cannot start: " << e.what() << std::endl;
        return 1;
    }

    AuthService auth_service(config.jwt_secret);
    RateLimitService rate_limit_service(config_loader, redis);

    RateLimitController rate_limit_controller(rate_limit_service, auth_service, *logger);
    AuthController auth_controller(auth_service);
    AdminController admin_controller(config_loader, rate_limit_service, redis);

    crow::SimpleApp app;

    // --- Core rate limiting endpoints ---
    CROW_ROUTE(app, "/request").methods("POST"_method)(
        [&](const crow::request& req) { return rate_limit_controller.handleRequest(req); });

    CROW_ROUTE(app, "/status").methods("GET"_method)(
        [&](const crow::request& req) { return rate_limit_controller.handleStatus(req); });

    // --- Auth endpoints ---
    CROW_ROUTE(app, "/login").methods("POST"_method)(
        [&](const crow::request& req) { return auth_controller.handleLogin(req); });

    CROW_ROUTE(app, "/apikey").methods("POST"_method)(
        [&](const crow::request& req) { return auth_controller.handleCreateApiKey(req); });

    // --- Admin / ops endpoints ---
    CROW_ROUTE(app, "/config").methods("POST"_method)(
        [&](const crow::request& req) { return admin_controller.handleReloadConfig(req); });

    CROW_ROUTE(app, "/health").methods("GET"_method)(
        [&](const crow::request& req) { return admin_controller.handleHealth(req); });

    // --- Metrics ---
    CROW_ROUTE(app, "/metrics").methods("GET"_method)(
        [&](const crow::request&) {
            crow::response res(200, MetricsRegistry::instance().render());
            res.add_header("Content-Type", "text/plain; version=0.0.4");
            return res;
        });

    std::cout << "Rate limiter server starting on port " << config.server_port
              << " (algorithm=" << RateLimiterFactory::create(config.algorithm, config.global_limit)->algorithmName()
              << ")" << std::endl;
    app.port(config.server_port).multithreaded().run();

    logger->stop();
    return 0;
}
