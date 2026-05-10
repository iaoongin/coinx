const { defineConfig, devices } = require('@playwright/test');
const path = require('path');

const projectRoot = path.resolve(__dirname, '..', '..');

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
    baseURL: 'http://localhost:5502',
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
    command: 'python src/coinx/web/app.py',
    cwd: projectRoot,
    env: {
      ...process.env,
      PYTHONUTF8: '1',
      PYTHONIOENCODING: 'utf-8',
      WEB_PASSWORD: 'admin123',
      WEB_PORT: '5502',
      PYTHONPATH: 'src',
    },
    port: 5502,
    timeout: 120000,
    reuseExistingServer: false,
  },
});
