'use client';

import './globals.css';
import Sidebar from '@/components/Sidebar';
import { useState, useEffect } from 'react';
import { FaChevronRight } from 'react-icons/fa';
import { motion } from 'framer-motion';

export default function RootLayout({
  children,
}: {
  children: any;
}) {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);

  useEffect(() => {
    const checkMobile = () => {
      setIsMobile(window.innerWidth < 768); // md breakpoint
    };
    checkMobile();
    window.addEventListener('resize', checkMobile);
    // 사이드바가 열린 상태로 화면 크기가 변경될 때를 대비하여 초기에도 isSidebarOpen을 false로 설정 (선택 사항)
    // if (window.innerWidth < 768) {
    //   setIsSidebarOpen(false);
    // }
    return () => window.removeEventListener('resize', checkMobile);
  }, []);

  return (
    <html lang="en">
      <head>
        <title>Investment Dashboard</title>
        <meta name="description" content="Portfolio strategies and screeners" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
      </head>
      <body>
        <div className="flex h-screen overflow-hidden bg-gray-100">
          {/* 모바일 헤더 */}
          <div className="fixed top-0 left-0 right-0 z-10 flex items-center justify-between p-4 bg-white shadow-md md:hidden">
            <motion.button
              onClick={() => setIsSidebarOpen(true)}
              className="p-2 text-gray-600 rounded-md hover:bg-gray-100"
              whileHover={{ scale: 1.1 }}
              whileTap={{ scale: 0.9 }}
            >
              <FaChevronRight />
            </motion.button>
            <h1 className="text-xl font-bold">Investment Dashboard</h1>
            <div className="w-8"></div> {/* 균형을 위한 빈 공간 */}
          </div>

          {/* 사이드바 */}
          {isSidebarOpen && (
            <>
              {/* 오버레이 (모바일에서만) */}
              {isMobile && (
                <motion.div
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 0.5 }}
                  transition={{ duration: 0.3 }}
                  className="fixed inset-0 z-20 bg-black"
                  onClick={() => setIsSidebarOpen(false)}
                />
              )}

              {/* 사이드바 컴포넌트 */}
              <Sidebar
                isOpen={isSidebarOpen}
                onClose={() => setIsSidebarOpen(false)}
              />
            </>
          )}

          {/* 메인 콘텐츠 */}
          <div className="flex flex-col flex-1 w-full h-full overflow-hidden">
            {/* 데스크톱 헤더 */}
            <div className="hidden p-4 bg-white shadow-sm md:block relative z-40">
              <div className="flex items-center justify-between">
                <motion.button
                  onClick={() => setIsSidebarOpen(!isSidebarOpen)}
                  className="p-2 text-gray-600 rounded-md hover:bg-gray-100 relative z-50"
                  whileHover={{ scale: 1.1 }}
                  whileTap={{ scale: 0.9 }}
                  style={{
                    transform: isSidebarOpen ? 'translateX(256px)' : 'translateX(0px)',
                    transition: 'transform 0.3s ease-in-out'
                  }}
                >
                  <FaChevronRight />
                </motion.button>
                <h1 className="text-2xl font-bold ml-auto">Investment Dashboard</h1>
              </div>
            </div>

            {/* 콘텐츠 영역 */}
            <main className="flex-1 p-4 overflow-auto mt-14 md:mt-0">
              {children}
            </main>
          </div>
        </div>
      </body>
    </html>
  );
}
