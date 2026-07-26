import type { NextConfig } from 'next';

const nextConfig: NextConfig = {
  // Produces a self-contained build in .next/standalone/ — ready for Dockerization
  output: 'standalone',

  // Expose NEXT_PUBLIC_WS_URL to the browser bundle if set at build time.
  // At runtime, it can also be provided via Docker environment variables
  // when using the standalone output mode.
  env: {
    NEXT_PUBLIC_WS_URL: process.env.NEXT_PUBLIC_WS_URL ?? '',
  },
};

export default nextConfig;
