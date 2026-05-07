import type { Metadata } from "next";
import "./globals.css";
import { ClerkProvider } from "@clerk/nextjs";
import { Sidebar } from "./components/Sidebar";

export const metadata: Metadata = {
  title: "MLcreate",
  description: "Train and deploy machine learning models on your CSV data",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <ClerkProvider>
      <html lang="en" className="h-full antialiased">
        <body className="h-full flex overflow-hidden bg-slate-50">
          <Sidebar />
          <div className="flex-1 overflow-y-auto">
            {children}
          </div>
        </body>
      </html>
    </ClerkProvider>
  );
}
