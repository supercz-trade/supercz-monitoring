module.exports = {
  apps: [
    {
      name:         "quantx",
      script:       "server.js",
      interpreter:  "node",

      cron_restart:       "*/30 * * * *", // restart setiap 30 menit
      max_memory_restart: "2000M",         // restart jika memory > 2GB
      autorestart:        true,
      max_restarts:       10,
      min_uptime:         "10s",
      restart_delay:      3000,

      env: {
        NODE_ENV: "production"
      },

      output: "./logs/out.log",
      error:  "./logs/error.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss"
    }
  ]
};