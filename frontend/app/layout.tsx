'use client';

import './globals.css';
import { Inter } from 'next/font/google';
import type { Metadata } from 'next';
import Sidebar from '@/components/Sidebar';
import { useState, useEffect } from 'react';
import { FaBars, FaArrowLeft } from 'react-icons/fa';
import { AnimatePresence, motion } from 'framer-motion';
import { usePathname } from 'next/navigation'; // usePathname 임포트

const inter = Inter({ subsets: ['latin'] });

export const metadata: Metadata = {
  title: 'Investment Dashboard',
  description: 'Portfolio strategies and screeners',
  viewport: 'width=device-width, initial-scale=1',
};

// 예시 데이터 (실제로는 API 등에서 가져와야 함)
const strategies = [
  { id: 'strategy1', name: 'Strategy Alpha' },
  { id: 'strategy2', name: 'Strategy Beta' },
  { id: 'strategy3', name: 'Strategy Gamma' },
  { id: 'strategy4', name: 'Strategy Delta' },
  { id: 'strategy5', name: 'Strategy Epsilon' },
  { id: 'strategy6', name: 'Strategy Zeta' },
];

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const pathname = usePathname(); // 현재 경로 가져오기

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

  const toggleSidebar = () => {
    setIsSidebarOpen(!isSidebarOpen);
  };

  // 사이드바가 열려있고, 모바일 뷰가 아닐 때만 왼쪽 마진을 적용
  const mainContentMargin = isSidebarOpen && !isMobile ? 'md:ml-64' : 'ml-0';

  return (
    <html lang="en">
      <body className={`${inter.className} flex h-screen overflow-hidden bg-gray-100`}>
        <Sidebar isOpen={isSidebarOpen} onClose={toggleSidebar} strategies={strategies} />
        <div className={`flex-1 flex flex-col transition-all duration-300 ease-in-out ${mainContentMargin}`}>
          <header className="bg-white shadow-md p-4">
            <div className="flex items-center">
              <button onClick={toggleSidebar} className="text-gray-700 p-2 rounded-md hover:bg-gray-200">
                {isSidebarOpen ? (
                  <FaArrowLeft size={24} />
                ) : (
                  <FaBars size={24} />
                )}
              </button>
              <h1 className="text-xl font-semibold ml-4 text-gray-800">Dashboard</h1>
            </div>
          </header>
          <AnimatePresence mode="wait">
            <motion.main
              key={pathname} // 경로가 바뀔 때마다 애니메이션 트리거
              initial={{ opacity: 0, y: 20 }} // 초기 상태 (투명하고 약간 아래에 위치)
              animate={{ opacity: 1, y: 0 }}   // 나타날 때의 상태 (불투명하고 제자리)
              exit={{ opacity: 0, y: -20 }}  // 사라질 때의 상태 (투명하고 약간 위로)
              transition={{ duration: 0.3 }} // 애니메이션 지속 시간
              className="flex-1 p-6 overflow-y-auto"
            >
              {children}
            </motion.main>
          </AnimatePresence>
        </div>
      </body>
    </html>
  );
}
// import { usePathname } from 'next/navigation'; // 이 줄을 삭제합니다.