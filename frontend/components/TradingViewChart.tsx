'use client';

import { useEffect, useRef, useMemo } from 'react';

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
  
  // 고유한 컨테이너 ID 생성
  const containerId = useMemo(() => 
    `tradingview_chart_${symbol}_${Math.random().toString(36).substr(2, 9)}`, 
    [symbol]
  );

  useEffect(() => {
    if (!symbol || symbol === 'N/A') return;
    
    const container = containerRef.current;
    if (!container) return;
    
    // 기존 내용 정리
    container.innerHTML = '';
    
    // TradingView 위젯 로드를 위한 타이머 설정
    const timer = setTimeout(() => {
      const script = document.createElement('script');
      script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';
      script.type = 'text/javascript';
      script.async = true;
      
      const config = {
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
        container_id: containerId,
        hide_side_toolbar: false,
        hide_top_toolbar: false,
        save_image: false,
        studies: [],
        show_popup_button: true,
        popup_width: '1000',
        popup_height: '650',
        withdateranges: true,
        hide_legend: false,
        hide_volume: false
      };
      
      script.innerHTML = JSON.stringify(config);
      
      // DOM에 컨테이너가 존재하는지 확인 후 스크립트 추가
      if (container && document.contains(container)) {
        container.appendChild(script);
      }
    }, 100); // 100ms 지연

    return () => {
      clearTimeout(timer);
      if (container) {
        container.innerHTML = '';
      }
    };
  }, [symbol, containerId]);

  if (!symbol || symbol === 'N/A') {
    return (
      <div className="flex items-center justify-center" style={{ width, height }}>
        <p className="text-gray-500">차트를 표시할 심볼이 없습니다.</p>
      </div>
    );
  }

  return (
    <div className="tradingview-widget-container bg-white rounded-lg" style={{ width, height }}>
      <div 
        ref={containerRef}
        id={containerId}
        style={{ width: '100%', height: 'calc(100% - 20px)' }}
      />
      <div className="tradingview-widget-copyright text-xs text-center py-1">
        <a 
          href={`https://kr.tradingview.com/symbols/${symbol}/`}
          rel="noopener nofollow" 
          target="_blank"
          className="text-blue-600 hover:text-blue-800"
        >
          TradingView에서 {symbol} 보기
        </a>
      </div>
    </div>
  );
};

export default TradingViewChart;