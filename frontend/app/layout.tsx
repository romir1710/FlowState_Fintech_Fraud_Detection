import type { Metadata } from 'next';

export const metadata: Metadata = {
  title: 'FlowState Monitor',
  description: 'Real-time transaction fraud monitoring dashboard',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
