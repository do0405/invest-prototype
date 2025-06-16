'use client';

import React from 'react';

import Link from 'next/link';
import { FaTimes, FaHome, FaChevronDown, FaChevronRight } from 'react-icons/fa';
import { useState } from 'react';

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
    <div
      className={`fixed inset-y-0 left-0 transform ${isOpen ? 'translate-x-0' : '-translate-x-full'}
                  transition-transform duration-300 ease-in-out 
                  bg-white text-gray-800 w-64 space-y-6 py-7 px-2 z-30 flex flex-col`}
    >
      <div className="flex items-center justify-between px-4 flex-shrink-0">
        <h2 className="flex-grow text-2xl font-semibold text-center">Strategies</h2>
        <Link 
          href="/"
          className="text-gray-800 hover:text-indigo-600 p-2 rounded-md transition-all duration-200 ease-in-out hover:bg-indigo-100 active:bg-indigo-200 transform hover:scale-105 active:scale-95"
        >
          <FaHome size={24} />
        </Link>
        <button onClick={onClose} className="md:hidden text-gray-800 hover:text-gray-600 ml-2">
          <FaTimes size={24} />
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto px-2">
        {/* Strategy Alpha 섹션 */}
        <div className="mb-4">
          <button
            onClick={() => setIsStrategyAlphaOpen(!isStrategyAlphaOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md transition-all duration-200 ease-in-out hover:bg-indigo-100 hover:text-indigo-700 active:bg-indigo-200"
          >
            <span className="font-semibold">Strategy Alpha</span>
            {isStrategyAlphaOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </button>
          
          {isStrategyAlphaOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <Link
                href="/strategy/all"
                className="block py-2 px-4 rounded-md text-sm transition-all duration-200 ease-in-out hover:bg-blue-50 hover:text-blue-700 border-l-2 border-blue-200 hover:border-blue-400"
              >
                📊 All Strategies Overview
              </Link>
              {strategyAlphaItems.map((item) => (
                <Link
                  key={item.id}
                  href={`/strategy/${item.id}`}
                  className={`block py-2 px-4 rounded-md text-sm transition-all duration-200 ease-in-out hover:bg-green-50 hover:text-green-700 border-l-2 ${
                    item.type === 'buy' ? 'border-green-200 hover:border-green-400' : 'border-red-200 hover:border-red-400'
                  }`}
                >
                  {item.type === 'buy' ? '📈' : '📉'} {item.name}
                </Link>
              ))}
            </div>
          )}
        </div>

        {/* Markminervini Screener 섹션 */}
        <div className="mb-4">
          <button
            onClick={() => setIsMarkminerviniOpen(!isMarkminerviniOpen)}
            className="w-full flex items-center justify-between py-2.5 px-4 rounded-md transition-all duration-200 ease-in-out hover:bg-purple-100 hover:text-purple-700 active:bg-purple-200"
          >
            <span className="font-semibold">Markminervini Screener</span>
            {isMarkminerviniOpen ? <FaChevronDown size={16} /> : <FaChevronRight size={16} />}
          </button>
          
          {isMarkminerviniOpen && (
            <div className="ml-4 mt-2 space-y-1">
              <Link
                href="/markminervini/all"
                className="block py-2 px-4 rounded-md text-sm transition-all duration-200 ease-in-out hover:bg-purple-50 hover:text-purple-700 border-l-2 border-purple-200 hover:border-purple-400"
              >
                🔍 All Screener Results
              </Link>
              {markminerviniItems.map((item) => (
                <Link
                  key={item.id}
                  href={`/markminervini/${item.id}`}
                  className="block py-2 px-4 rounded-md text-sm transition-all duration-200 ease-in-out hover:bg-purple-50 hover:text-purple-700 border-l-2 border-purple-200 hover:border-purple-400"
                >
                  {item.icon} {item.name}
                </Link>
              ))}
            </div>
          )}
        </div>

        {/* 기존 전략들 */}
        <div className="border-t pt-4">
          <h3 className="px-4 text-sm font-semibold text-gray-500 uppercase tracking-wider mb-2">Other Strategies</h3>
          {strategies.filter(s => !['strategy1', 'strategy2', 'strategy3', 'strategy4', 'strategy5', 'strategy6'].includes(s.id)).map((strategy) => (
            <Link
              key={strategy.id}
              href={`/strategy/${strategy.id}`}
              className="block py-2.5 px-4 rounded-md transition-all duration-200 ease-in-out hover:bg-indigo-100 hover:text-indigo-700 active:bg-indigo-200 transform hover:translate-x-1 active:translate-x-0.5"
            >
              {strategy.name}
            </Link>
          ))}
          <Link
            href="/volatility-skew"
            className="block py-2.5 px-4 rounded-md transition-all duration-200 ease-in-out hover:bg-yellow-50 hover:text-yellow-700 active:bg-yellow-200 transform hover:translate-x-1 active:translate-x-0.5"
          >
            ⚡ Volatility Skew Screener
          </Link>
        </div>
      </nav>
    </div>
  );
};

export default Sidebar;