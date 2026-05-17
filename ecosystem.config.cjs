module.exports = {
  apps: [
    {
      name: 'clipflow',
      script: './server.js',
      cwd: '/var/www/clipflow',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '512M',
      env: {
        NODE_ENV: 'production',
        PORT: 3019
      }
    }
  ]
};
