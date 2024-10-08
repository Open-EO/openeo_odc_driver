bind     = "0.0.0.0:5000"
workers  = 3
threads  = 1
timeout  = 1000000
max_requests = 1

logconfig_dict = {
    "root": {"handlers": ["error_console"], "level": "INFO"},
    "loggers": {},
}