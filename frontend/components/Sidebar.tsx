'use client';

import React from 'react';

import Link from 'next/link';
import { motion } from 'framer-motion';
import { FaTimes, FaHome, FaChevronDown, FaChevronRight } from 'react-icons/fa';
import { useState } from 'react';

const MotionLink = motion(Link);

// Strategy 인터페이스 정의 추가
interface Strategy {
  id: string;
  name: string;
}

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
  strategies: Strategy[];
}

const Sidebar: React.FC<SidebarProps> = ({ isOpen, onClose, strategies }) => {
  const [isStrategyAlphaOpen, setIsStrategyAlphaOpen] = useState(false);
  const [isScreenersOpen, setIsScreenersOpen] = useState(false);
  const [isAnalysisOpen, setIsAnalysisOpen] = useState(false);
  const [isResourcesOpen, setIsResourcesOpen] = useState(false);
  const [isMarkMinerviniOpen, setIsMarkMinerviniOpen] = useState(false);
  const [isQullamaggieOpen, setIsQullamaggieOpen] = useState(false);

  const strategyAlphaItems = [
    { id: 'strategy1', name: 'Strategy 1 (Buy)', type: 'buy' },
    { id: 'strategy2', name: 'Strategy 2 (Sell)', type: 'sell' },
    { id: 'strategy3', name: 'Strategy 3 (Buy)', type: 'buy' },
    { id: 'strategy4', name: 'Strategy 4 (Buy)', type: 'buy' },
    { id: 'strategy5', name: 'Strategy 5 (Buy)', type: 'buy' },
    { id: 'strategy6', name: 'Strategy 6 (Sell)', type: 'sell' },
  ];

  const markminerviniItems = [
    { id: 'image_pattern_results', name: '이미지 패턴 분석', icon: '🖼️' },
    { id: 'integrated_pattern_results', name: '통합 패턴 분석', icon: '🔗' },
    { id: 'integrated_results', name: '최종 통합 결과', icon: '🎯' },
  ];

  const qullamaggieItems = [
    { id: 'breakout', name: 'Breakout Setup', icon: '🚀' },
    { id: 'episode-pivot', name: 'Episode Pivot', icon: '🎯' },
    { id: 'parabolic-short', name: 'Parabolic Short', icon: '📉' },
    { id: 'buy-signals', name: 'Buy Signals', icon: '🟢' },
    { id: 'sell-signals', name: 'Sell Signals', icon: '🔴' },
  ];

  const screenerItems = [
    { 
      id: 'markminervini', 
      name: 'Mark Minervini', 
      icon: '📊', 
      items: [
        ...markminerviniItems,
        { id: 'financial_screening', name: 'Financial Screening', icon: '💰', href: '/screening/financial' },
        { id: 'integrated_screening', name: 'Integrated Screening', icon: '🔗', href: '/screening/integrated' },
      ]
    },
    { id: 'qullamaggie', name: 'Qullamaggie', icon: '🎯', items: [
      { id: 'breakout', name: 'Breakout Setup', icon: '🚀' },
      { id: 'episode-pivot', name: 'Episode Pivot', icon: '🎯' },
      { id: 'parabolic-short', name: 'Parabolic Short', icon: '📉' },
      { id: 'buy-signals', name: 'Buy Signals', icon: '🟢' },
      { id: 'sell-signals', name: 'Sell Signals', icon: '🔴' },
    ]},
    { id: 'us-setup', name: 'US Setup', icon: '⚙️', href: '/us-setup' },
    { id: 'us-gainers', name: 'US Gainers', icon: '📈', href: '/us-gainers' },
    { id: 'volatility-skew', name: 'Volatility Skew', icon: '⚡', href: '/volatility-skew' },
    { id: 'momentum', name: 'Momentum Signals', icon: '📈', href: '/strategy/momentum-signals' },
    { id: 'leader-stock', name: 'Leader Stock', icon: '👑', href: '/strategy/leader-stock' },
    { id: 'ipo-investment', name: 'IPO Investment', icon: '🆕', href: '/strategy/ipo-investment' },
  ];

  const analysisItems = [
    { id: 'market-regime', name: 'Market Regime', icon: '🌊', href: '/strategy/market-regime' },
  ];

  const resourceItems = [
    { id: 'all-resources', name: '📚 전체 사이트 모음', icon: '🔗', href: '/resources', external: false },
    { id: 'tradingview-screener', name: 'TradingView 스크리너', icon: '📊', href: 'https://kr.tradingview.com/screener/', external: true },
  ];

  return (
    <motion.div
      initial={{ x: '-100%' }}
      animate={{ x: isOpen ? '0%' : '-100%' }}
      transition={{ type: 'spring', stiffness: 300, damping: 30 }}
      className="fixed inset-y-0 left-0 bg-white text-gray-800 w-64 space-y-6 py-7 px-2 z-30 flex flex-col shadow-lg"
    >
      <div className="flex items-center justify-between px-4 flex-shrink-0">
        <h2 className="flex-grow text-2xl font-semibold text-center">Strategies</h2>
        <MotionLink
          href="/"
          className="text-gray-800 p-2 rounded-md"
          whileHover={{ scale: 1.05, backgroundColor: 'rgba(129, 140, 248, 0.1)' }}
          whileTap={{ scale: 0.95, backgroundColor: 'rgba(129, 140, 248, 0.2)' }}
          transition={{ type: 'spring', stiffness: 400, damping: 17 }}
        >
          <FaHome size={24} />
        </MotionLink>
        <button onClick={onClose} className="md:hidden text-gray-800 hover:text-gray-600 ml-2">
          <FaTimes size={24} />
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto px-2">
        {/* 핵심 기능 섹션 */}
        <div className="mb-6 border-b pb-4">
          <h3 className="px-4 text-sm font-semibold text-gray-500 uppercase tracking-wider mb-3">핵심 기능</h3>
          <MotionLink
            href="/recent-signals"
            className="block py-2.5 px-4 rounded-md mb-2"
            whileHover={{ backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#2563eb', x: 4 }}
            whileTap={{ backgroundColor: 'rgba(59, 130, 246, 0.2)', x: 2 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            🔥 최근 시그널
          </MotionLink>
          <MotionLink
            href="/top-recommendations"
            className="block py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: '#16a34a', x: 4 }}
            whileTap={{ backgroundColor: 'rgba(34, 197, 94, 0.2)', x: 2 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            ⭐ Top 10 매수 랭킹
          </MotionLink>
        </div>
        {/* Strategy Alpha 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsStrategyAlphaOpen(!isStrategyAlphaOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5' }}
            whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">💼 Portfolio Strategies</span>
            {isStrategyAlphaOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </motion.button>
          
          {isStrategyAlphaOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <MotionLink
                  href="/strategy/all"
                  className="block py-2 px-4 rounded-md text-sm"
                  whileHover={{ backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#2563eb', borderLeftColor: '#60a5fa' }}
                  whileTap={{ backgroundColor: 'rgba(59, 130, 246, 0.2)' }}
                  transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                >
                  📊 All Strategies Overview
                </MotionLink>
                {strategyAlphaItems.map((item) => (
                  <MotionLink
                    key={item.id}
                    href={`/strategy/${item.id}`}
                    className={`block py-2 px-4 rounded-md text-sm ${
                      item.type === 'buy' ? 'border-l-2 border-green-200' : 'border-l-2 border-red-200'
                    }`}
                    whileHover={{ backgroundColor: item.type === 'buy' ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)', color: item.type === 'buy' ? '#16a34a' : '#dc2626', borderLeftColor: item.type === 'buy' ? '#4ade80' : '#f87171' }}
                    whileTap={{ backgroundColor: item.type === 'buy' ? 'rgba(34, 197, 94, 0.2)' : 'rgba(239, 68, 68, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    {item.type === 'buy' ? '📈' : '📉'} {item.name}
                  </MotionLink>
                ))} 
              </motion.div>
            </div>
          )}
        </div>

        {/* 스크리너 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsScreenersOpen(!isScreenersOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(168, 85, 247, 0.1)', color: '#7e22ce' }}
            whileTap={{ backgroundColor: 'rgba(168, 85, 247, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">🔍 Stock Screeners</span>
            {isScreenersOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </motion.button>
          
          {isScreenersOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {/* Mark Minervini 카테고리 */}
                <div className="mb-3">
                  <motion.button
                    onClick={() => setIsMarkMinerviniOpen(!isMarkMinerviniOpen)}
                    className="w-full flex items-center justify-between py-2 px-4 rounded-md text-sm"
                    whileHover={{ backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#2563eb' }}
                    whileTap={{ backgroundColor: 'rgba(59, 130, 246, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    <span className="font-medium">📊 Mark Minervini</span>
                    {isMarkMinerviniOpen ? <FaChevronDown size={12} /> : <FaChevronRight size={12} />}
                  </motion.button>
                  
                  {isMarkMinerviniOpen && (
                    <div className="ml-4 mt-1 space-y-1">
                      {markminerviniItems.map((item) => (
                        <MotionLink
                          key={item.id}
                          href={`/markminervini/${item.id}`}
                          className="block py-1.5 px-3 rounded-md text-xs"
                          whileHover={{ backgroundColor: 'rgba(59, 130, 246, 0.1)', color: '#2563eb' }}
                          whileTap={{ backgroundColor: 'rgba(59, 130, 246, 0.2)' }}
                          transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                        >
                          {item.icon} {item.name}
                        </MotionLink>
                      ))}
                    </div>
                  )}
                </div>

                {/* Qullamaggie 카테고리 */}
                <div className="mb-3">
                  <motion.button
                    onClick={() => setIsQullamaggieOpen(!isQullamaggieOpen)}
                    className="w-full flex items-center justify-between py-2 px-4 rounded-md text-sm"
                    whileHover={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: '#16a34a' }}
                    whileTap={{ backgroundColor: 'rgba(34, 197, 94, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    <span className="font-medium">🎯 Qullamaggie</span>
                    {isQullamaggieOpen ? <FaChevronDown size={12} /> : <FaChevronRight size={12} />}
                  </motion.button>
                  
                  {isQullamaggieOpen && (
                    <div className="ml-4 mt-1 space-y-1">
                      {qullamaggieItems.map((item) => (
                        <MotionLink
                          key={item.id}
                          href={`/qullamaggie/${item.id}`}
                          className="block py-1.5 px-3 rounded-md text-xs"
                          whileHover={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: '#16a34a' }}
                          whileTap={{ backgroundColor: 'rgba(34, 197, 94, 0.2)' }}
                          transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                        >
                          {item.icon} {item.name}
                        </MotionLink>
                      ))}
                    </div>
                  )}
                </div>

                {/* 기타 스크리너들 */}
                {screenerItems.filter(item => !item.items).map((screener) => (
                  <MotionLink
                    key={screener.id}
                    href={screener.href}
                    className="block py-2 px-4 rounded-md text-sm"
                    whileHover={{ backgroundColor: 'rgba(168, 85, 247, 0.1)', color: '#7e22ce' }}
                    whileTap={{ backgroundColor: 'rgba(168, 85, 247, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    {screener.icon} {screener.name}
                  </MotionLink>
                ))}
              </motion.div>
            </div>
          )}
        </div>

        {/* 분석 도구 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsAnalysisOpen(!isAnalysisOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: '#16a34a' }}
            whileTap={{ backgroundColor: 'rgba(34, 197, 94, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">📊 Analysis Tools</span>
            {isAnalysisOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </motion.button>

          {isAnalysisOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {analysisItems.map((item) => (
                  <MotionLink
                    key={item.id}
                    href={item.href}
                    className="block py-2 px-4 rounded-md text-sm"
                    whileHover={{ backgroundColor: 'rgba(34, 197, 94, 0.1)', color: '#16a34a', borderLeftColor: '#4ade80' }}
                    whileTap={{ backgroundColor: 'rgba(34, 197, 94, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    {item.icon} {item.name}
                  </MotionLink>
                ))}
              </motion.div>
            </div>
          )}
        </div>

        {/* 참고 자료 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsResourcesOpen(!isResourcesOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(234, 179, 8, 0.1)', color: '#a16207' }}
            whileTap={{ backgroundColor: 'rgba(234, 179, 8, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">🔗 Reference Sites</span>
            {isResourcesOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </motion.button>

          {isResourcesOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                {resourceItems.map((item) => (
                  <MotionLink
                    key={item.id}
                    href={item.href}
                    target={item.external ? '_blank' : undefined}
                    rel={item.external ? 'noopener noreferrer' : undefined}
                    className="block py-2 px-4 rounded-md text-sm flex items-center justify-between"
                    whileHover={{ backgroundColor: 'rgba(234, 179, 8, 0.1)', color: '#a16207', borderLeftColor: '#facc15' }}
                    whileTap={{ backgroundColor: 'rgba(234, 179, 8, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    <span>{item.icon} {item.name}</span>
                    {item.external && <span className="text-xs opacity-60">↗</span>}
                  </MotionLink>
                ))}
              </motion.div>
            </div>
          )}
        </div>

        {/* 기타 도구들 */}
        <div className="border-t pt-4">
          <h3 className="px-4 text-sm font-semibold text-gray-500 uppercase tracking-wider mb-2">🛠️ Additional Tools</h3>
          <MotionLink
            href="/embedded-screener"
            className="block py-2.5 px-4 rounded-md mb-2"
            whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5', x: 4 }}
            whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)', x: 2 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            🖥️ 임베디드 스크리너
          </MotionLink>
          <MotionLink
            href="/screening/technical"
            className="block py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5', x: 4 }}
            whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)', x: 2 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            ⚙️ Technical Screening
          </MotionLink>
        </div>
      </nav>
    </motion.div>
  );
};

export default Sidebar;