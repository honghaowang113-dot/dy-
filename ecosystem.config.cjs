module.exports = {
  apps: [
    {
      name: 'clipflow',
      script: './server.js',
      cwd: process.env.APP_DIR || '/var/www/clipflow',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        PORT: Number(process.env.PORT || process.env.APP_PORT || 3019)
      }
    }
  ]
};
