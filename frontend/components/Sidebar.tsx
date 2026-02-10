'use client';

import Link from 'next/link';
import { motion } from 'framer-motion';
import { FaTimes, FaHome, FaChevronDown, FaChevronRight } from 'react-icons/fa';
import { useState } from 'react';

const MotionLink = motion.create(Link);

interface SidebarProps {
  isOpen: boolean;
  onClose: () => void;
}

const Sidebar = ({ isOpen, onClose }: SidebarProps) => {
  const [isStrategyAlphaOpen, setIsStrategyAlphaOpen] = useState(false);
  const [isScreenersOpen, setIsScreenersOpen] = useState(false);
  const [isAnalysisOpen, setIsAnalysisOpen] = useState(false);
  const [isResourcesOpen, setIsResourcesOpen] = useState(false);
  const [isMarkMinerviniOpen, setIsMarkMinerviniOpen] = useState(false);
  const [isQullamaggieOpen, setIsQullamaggieOpen] = useState(false);

  const strategyAlphaItems: never[] = [];

  const markminerviniItems = [
    { id: 'pattern_detection_results', name: '이미지 패턴 분석', icon: '🖼️' },
    { id: 'integrated_pattern_results', name: '통합 패턴 분석', icon: '🔗' },
    { id: 'integrated_results', name: '패턴 인식 전 결과', icon: '🎯' },
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
    { id: 'momentum', name: 'Momentum Signals', icon: '📈', href: '/strategy/momentum-signals' },
    { id: 'leader-stock', name: 'Leader Stock', icon: '👑', href: '/strategy/leader-stock' },
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
      className="fixed inset-y-0 left-0 bg-card border-r border-border text-foreground w-64 space-y-6 py-7 px-2 z-30 flex flex-col shadow-lg"
    >
      <div className="flex items-center justify-between px-4 flex-shrink-0">
        <h2 className="flex-grow text-xl font-semibold text-center text-foreground">Strategies</h2>
        <MotionLink
          href="/"
          className="text-muted-foreground p-2 rounded-notion hover:bg-muted"
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.95 }}
          transition={{ type: 'spring', stiffness: 400, damping: 17 }}
        >
          <FaHome size={20} />
        </MotionLink>
        <button onClick={onClose} className="md:hidden text-muted-foreground hover:text-foreground ml-2">
          <FaTimes size={20} />
        </button>
      </div>

      <nav className="flex-1 overflow-y-auto px-2">
        {/* 핵심 기능 섹션 */}
        <div className="mb-6 border-b border-border pb-4">
          <h3 className="px-4 text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">핵심 기능</h3>
          <MotionLink
            href="/recent-signals"
            className="block py-2 px-4 rounded-notion text-sm text-muted-foreground hover:bg-muted hover:text-foreground"
            whileHover={{ x: 2 }}
            whileTap={{ x: 1 }}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            🔥 최근 시그널
          </MotionLink>
        </div>
        {/* Legacy Strategy 섹션 제거됨 */}

        {/* 스크리너 섹션 */}
        <div className="mb-4">
          <motion.button
            onClick={() => setIsScreenersOpen(!isScreenersOpen)}
            className="w-full flex items-center justify-between py-2 px-4 rounded-notion text-sm font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
            whileHover={{}}
            whileTap={{}}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span>🔍 Stock Screeners</span>
            {isScreenersOpen ? <FaChevronDown size={14} /> : <FaChevronRight size={14} />}
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
                    className="w-full flex items-center justify-between py-2 px-4 rounded-notion text-sm font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
                    whileHover={{}}
                    whileTap={{}}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    <span>📊 Mark Minervini</span>
                    {isMarkMinerviniOpen ? <FaChevronDown size={12} /> : <FaChevronRight size={12} />}
                  </motion.button>
                  
                  {isMarkMinerviniOpen && (
                    <div className="ml-4 mt-1 space-y-1">
                      {markminerviniItems.map((item) => (
                        <MotionLink
                          key={item.id}
                          href={`/markminervini/${item.id}`}
                          className="block py-1.5 px-3 rounded-notion text-xs text-muted-foreground hover:bg-muted hover:text-foreground"
                          whileHover={{ x: 2 }}
                          whileTap={{ x: 1 }}
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
                    className="w-full flex items-center justify-between py-2 px-4 rounded-notion text-sm font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
                    whileHover={{}}
                    whileTap={{}}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    <span>🎯 Qullamaggie</span>
                    {isQullamaggieOpen ? <FaChevronDown size={12} /> : <FaChevronRight size={12} />}
                  </motion.button>
                  
                  {isQullamaggieOpen && (
                    <div className="ml-4 mt-1 space-y-1">
                      {qullamaggieItems.map((item) => (
                        <MotionLink
                          key={item.id}
                          href={`/qullamaggie/${item.id}`}
                          className="block py-1.5 px-3 rounded-notion text-xs text-muted-foreground hover:bg-muted hover:text-foreground"
                          whileHover={{ x: 2 }}
                          whileTap={{ x: 1 }}
                          transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                        >
                          {item.icon} {item.name}
                        </MotionLink>
                      ))}
                    </div>
                  )}
                </div>

                {/* 기타 스크리너들 (US Setup, US Gainers 제거) */}
                {screenerItems.filter(item => !item.items && !['us-setup', 'us-gainers'].includes(item.id)).map((screener) => (
                  <MotionLink
                    key={screener.id}
                    href={screener.href}
                    className="block py-2 px-4 rounded-notion text-sm text-muted-foreground hover:bg-muted hover:text-foreground"
                    whileHover={{ x: 2 }}
                    whileTap={{ x: 1 }}
                    transition={{ type: 'spring', stiffness: 400, damping: 17 }}
                  >
                    {screener.icon} {screener.name}
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
            className="w-full flex items-center justify-between py-2 px-4 rounded-notion text-sm font-medium text-muted-foreground hover:bg-muted hover:text-foreground"
            whileHover={{}}
            whileTap={{}}
            transition={{ type: 'spring', stiffness: 400, damping: 17 }}
          >
            <span>🔗 Reference Sites</span>
            {isResourcesOpen ? <FaChevronDown size={14} /> : <FaChevronRight size={14} />}
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
                    className="block py-2 px-4 rounded-notion text-sm flex items-center justify-between text-muted-foreground hover:bg-muted hover:text-foreground"
                    whileHover={{ x: 2 }}
                    whileTap={{ x: 1 }}
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


      </nav>
    </motion.div>
  );
};

export default Sidebar;
