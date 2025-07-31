'use client';

import React, { useState } from 'react';
import { motion } from 'framer-motion';
import { FaExternalLinkAlt, FaExpand, FaCompress } from 'react-icons/fa';

interface ScreenerOption {
  id: string;
  name: string;
  url: string;
  description: string;
  icon: string;
}

const screenerOptions: ScreenerOption[] = [
  {
    id: 'yahoo-finance',
    name: 'Yahoo Finance 스크리너',
    url: 'https://finance.yahoo.com/screener/',
    description: '야후 파이낸스 주식 스크리너',
    icon: '🟣'
  },
  {
    id: 'marketwatch',
    name: 'MarketWatch 스크리너',
    url: 'https://www.marketwatch.com/tools/screener',
    description: '마켓워치 스크리닝 도구',
    icon: '📈'
  },
  {
    id: 'investing',
    name: 'Investing.com 스크리너',
    url: 'https://www.investing.com/stock-screener/',
    description: 'Investing.com 주식 스크리너',
    icon: '💹'
  }
];

const EmbeddedScreenerPage: React.FC = () => {
  const [selectedScreener, setSelectedScreener] = useState<ScreenerOption>(screenerOptions[0]);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [isLoading, setIsLoading] = useState(true);

  const handleScreenerChange = (screener: ScreenerOption) => {
    setSelectedScreener(screener);
    setIsLoading(true);
  };

  const handleIframeLoad = () => {
    setIsLoading(false);
  };

  const toggleFullscreen = () => {
    setIsFullscreen(!isFullscreen);
  };

  return (
    <div className="min-h-screen bg-gray-50">
      {/* 헤더 */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.6 }}
        className="bg-white shadow-sm border-b border-gray-200 p-4"
      >
        <div className="max-w-7xl mx-auto">
          <div className="flex flex-col lg:flex-row lg:items-center lg:justify-between gap-4">
            <div>
              <h1 className="text-2xl font-bold text-gray-900 mb-2">
                📊 임베디드 스크리너
              </h1>
              <p className="text-gray-600">
                외부 스크리닝 도구를 대시보드 내에서 직접 사용하세요
              </p>
            </div>
            
            {/* 스크리너 선택 */}
            <div className="flex flex-wrap gap-2">
              {screenerOptions.map((option) => (
                <button
                  key={option.id}
                  onClick={() => handleScreenerChange(option)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 flex items-center gap-2 ${
                    selectedScreener.id === option.id
                      ? 'bg-blue-600 text-white shadow-md'
                      : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                  }`}
                >
                  <span>{option.icon}</span>
                  <span className="hidden sm:inline">{option.name}</span>
                </button>
              ))}
            </div>
          </div>
        </div>
      </motion.div>

      {/* 컨트롤 바 */}
      <div className="bg-white border-b border-gray-200 p-3">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-4">
            <span className="text-sm text-gray-600">
              현재 보기: <span className="font-medium">{selectedScreener.name}</span>
            </span>
            <span className="text-xs text-gray-500">
              {selectedScreener.description}
            </span>
          </div>
          
          <div className="flex items-center gap-2">
            <button
              onClick={toggleFullscreen}
              className="p-2 text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
              title={isFullscreen ? '축소' : '전체화면'}
            >
              {isFullscreen ? <FaCompress /> : <FaExpand />}
            </button>
            
            <a
              href={selectedScreener.url}
              target="_blank"
              rel="noopener noreferrer"
              className="p-2 text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
              title="새 탭에서 열기"
            >
              <FaExternalLinkAlt />
            </a>
          </div>
        </div>
      </div>

      {/* 임베디드 스크리너 */}
      <div className={`relative ${isFullscreen ? 'fixed inset-0 z-50 bg-white' : 'h-screen'}`}>
        {isLoading && (
          <div className="absolute inset-0 flex items-center justify-center bg-gray-50">
            <div className="text-center">
              <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto mb-4"></div>
              <p className="text-gray-600">스크리너를 로딩 중...</p>
            </div>
          </div>
        )}
        
        <iframe
          src={selectedScreener.url}
          className={`w-full border-0 ${isFullscreen ? 'h-full' : 'h-full'}`}
          onLoad={handleIframeLoad}
          title={selectedScreener.name}
          sandbox="allow-same-origin allow-scripts allow-popups allow-forms"
        />
      </div>
    </div>
  );
};

export default EmbeddedScreenerPage;