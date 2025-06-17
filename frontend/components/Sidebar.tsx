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
  const [isMarkminerviniOpen, setIsMarkminerviniOpen] = useState(false);

  const strategyAlphaItems = [
    { id: 'strategy1', name: 'Strategy 1 (Buy)', type: 'buy' },
    { id: 'strategy2', name: 'Strategy 2 (Sell)', type: 'sell' },
    { id: 'strategy3', name: 'Strategy 3 (Buy)', type: 'buy' },
    { id: 'strategy4', name: 'Strategy 4 (Buy)', type: 'buy' },
    { id: 'strategy5', name: 'Strategy 5 (Buy)', type: 'buy' },
    { id: 'strategy6', name: 'Strategy 6 (Sell)', type: 'sell' },
  ];

  const markminerviniItems = [
    { id: 'advanced_financial_results', name: 'Advanced Financial Results', icon: '💰' },
    { id: 'integrated_results', name: 'Integrated Results', icon: '🔗' },
    { id: 'new_tickers', name: 'New Tickers', icon: '🆕' },
    { id: 'previous_us_with_rs', name: 'Previous US with RS', icon: '📈' },
    { id: 'us_with_rs', name: 'US with RS', icon: '🇺🇸' },
    { id: 'pattern_analysis_results', name: 'Pattern Analysis', icon: '📊' },
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
        {/* Strategy Alpha 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsStrategyAlphaOpen(!isStrategyAlphaOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5' }}
            whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">Strategy Alpha</span>
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

        {/* Markminervini Screener 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsMarkminerviniOpen(!isMarkminerviniOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(168, 85, 247, 0.1)', color: '#7e22ce' }}
            whileTap={{ backgroundColor: 'rgba(168, 85, 247, 0.2)' }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span className="font-semibold">Markminervini Screener</span>
            {isMarkminerviniOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </motion.button>
          
          {isMarkminerviniOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <motion.div
                initial={{ opacity: 0, y: -10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -10 }}
                transition={{ duration: 0.2 }}
              >
                <MotionLink
                  href="/markminervini/all"
                  className="block py-2 px-4 rounded-md text-sm"
                  whileHover={{ backgroundColor: 'rgba(168, 85, 247, 0.1)', color: '#7e22ce', borderLeftColor: '#c084fc' }}
                  whileTap={{ backgroundColor: 'rgba(168, 85, 247, 0.2)' }}
                  transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                >
                  🔍 All Screener Results
                </MotionLink>
                {markminerviniItems.map((item) => (
                  <MotionLink
                    key={item.id}
                    href={`/markminervini/${item.id}`}
                    className="block py-2 px-4 rounded-md text-sm"
                    whileHover={{ backgroundColor: 'rgba(168, 85, 247, 0.1)', color: '#7e22ce', borderLeftColor: '#c084fc' }}
                    whileTap={{ backgroundColor: 'rgba(168, 85, 247, 0.2)' }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    {item.icon} {item.name}
                  </MotionLink>
                ))} 
              </motion.div>
            </div>
          )}
        </div>

        {/* 기존 전략들 */}
        <div className="border-t pt-4">
          <h3 className="px-4 text-sm font-semibold text-gray-500 uppercase tracking-wider mb-2">Other Strategies</h3>
          {strategies.filter(s => !['strategy1', 'strategy2', 'strategy3', 'strategy4', 'strategy5', 'strategy6'].includes(s.id)).map((strategy) => (
            <MotionLink
              key={strategy.id}
              href={`/strategy/${strategy.id}`}
              className="block py-2.5 px-4 rounded-md"
              whileHover={{ backgroundColor: 'rgba(129, 140, 248, 0.1)', color: '#4f46e5', x: 4 }}
              whileTap={{ backgroundColor: 'rgba(129, 140, 248, 0.2)', x: 2 }}
              transition={{ type: 'spring', stiffness: 400, damping: 17 }}
            >
              {strategy.name}
            </MotionLink>
          ))}
          <MotionLink
            href="/volatility-skew"
            className="block py-2.5 px-4 rounded-md"
            whileHover={{ backgroundColor: 'rgba(250, 204, 21, 0.1)', color: '#a16207', x: 4 }}
            whileTap={{ backgroundColor: 'rgba(250, 204, 21, 0.2)', x: 2 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            ⚡ Volatility Skew Screener
          </MotionLink>
        </div>
      </nav>
    </motion.div>
  );
};

export default Sidebar;