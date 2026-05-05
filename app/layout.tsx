import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "ML Create CSV Upload",
  description: "Minimal CSV upload prototype with a dummy Python backend",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="h-full antialiased">
      <body className="min-h-full flex flex-col">{children}</body>
    </html>
  );
}
