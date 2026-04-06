const { defineConfig, devices } = require('@playwright/test');

module.exports = defineConfig({
  testDir: './tests',
  timeout: 60000,
  fullyParallel: false,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: 1,
  outputDir: './.artifacts/results',
  reporter: [['html', { outputFolder: './.artifacts/report', open: 'never' }]],
  use: {
    baseURL: 'http://localhost:5500',
    trace: 'on-first-retry',
    headless: true,
    viewport: { width: 1280, height: 720 },
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'cd ../.. && PYTHONPATH=src python src/coinx/web/app.py',
    port: 5500,
    timeout: 120000,
    reuseExistingServer: !process.env.CI,
  },
});
