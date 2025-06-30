'use client';

import React from 'react';

import Link from 'next/link';
import { motion } from 'framer-motion';
import { FaTimes, FaHome } from 'react-icons/fa';
import StrategyGroup, { StrategyGroupItem } from './StrategyGroup';

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

  const strategyAlphaItems: StrategyGroupItem[] = [
    { id: 'all', name: '📊 All Strategies Overview' },
    { id: 'strategy1', name: 'Strategy 1 (Buy)', type: 'buy' },
    { id: 'strategy2', name: 'Strategy 2 (Sell)', type: 'sell' },
    { id: 'strategy3', name: 'Strategy 3 (Buy)', type: 'buy' },
    { id: 'strategy4', name: 'Strategy 4 (Buy)', type: 'buy' },
    { id: 'strategy5', name: 'Strategy 5 (Buy)', type: 'buy' },
    { id: 'strategy6', name: 'Strategy 6 (Sell)', type: 'sell' },
  ];

  const markminerviniItems: StrategyGroupItem[] = [
    { id: 'all', name: '🔍 All Screener Results' },
    { id: 'advanced_financial_results', name: 'Advanced Financial Results', icon: '💰' },
    { id: 'integrated_results', name: 'Integrated Results', icon: '🔗' },
    { id: 'new_tickers', name: 'New Tickers', icon: '🆕' },
    { id: 'previous_us_with_rs', name: 'Previous US with RS', icon: '📈' },
    { id: 'us_with_rs', name: 'US with RS', icon: '🇺🇸' },
    { id: 'us_setup_results', name: 'US Setup Results', icon: '⚙️' },
    { id: 'us_gainers_results', name: 'US Gainers Results', icon: '📈' },
    { id: 'pattern_detection_results', name: 'Pattern Detection', icon: '📊' },
  ];

  const otherStrategies: StrategyGroupItem[] = [
    ...strategies
      .filter(s => !['strategy1','strategy2','strategy3','strategy4','strategy5','strategy6'].includes(s.id))
      .map(s => ({ id: s.id, name: s.name })),
    { id: 'volatility-skew', name: '⚡ Volatility Skew Screener' },
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
        <StrategyGroup title="Strategy Alpha" items={strategyAlphaItems} basePath="/strategy" />
        <StrategyGroup title="Markminervini Screener" items={markminerviniItems} basePath="/markminervini" />
        <div className="border-t pt-4">
          <StrategyGroup title="Other Strategies" items={otherStrategies} basePath="/strategy" />
        </div>
      </nav>
    </motion.div>
  );
};

export default Sidebar;