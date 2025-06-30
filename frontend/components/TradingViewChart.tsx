'use client';

import { useEffect, useRef } from 'react';

interface TradingViewChartProps {
  symbol: string;
  width?: string;
  height?: string;
}

const TradingViewChart: React.FC<TradingViewChartProps> = ({ 
  symbol, 
  width = '100%', 
  height = '400px' 
}) => {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const container = containerRef.current;
    const script = document.createElement('script');
    script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';
    script.type = 'text/javascript';
    script.async = true;
    script.innerHTML = JSON.stringify({
      autosize: true,
      symbol: symbol,
      interval: 'D',
      timezone: 'Etc/UTC',
      theme: 'light',
      style: '1',
      locale: 'en',
      toolbar_bg: '#f1f3f6',
      enable_publishing: false,
      allow_symbol_change: true,
      container_id: 'tradingview_chart'
    });

    if (container) {
      container.appendChild(script);
    }

    return () => {
      if (container) {
        container.innerHTML = '';
      }
    };
  }, [symbol, width, height]);

  return (
    <div className="tradingview-widget-container" style={{ width, height }}>
      <div 
        ref={containerRef}
        id="tradingview_chart"
        style={{ width: '100%', height: '100%' }}
      />
      <div className="tradingview-widget-copyright">
        <a 
          href="https://kr.tradingview.com/" 
          rel="noopener nofollow" 
          target="_blank"
        >
          <span className="blue-text">TradingView에서 추적</span>
        </a>
      </div>
    </div>
  );
};

export default TradingViewChart;