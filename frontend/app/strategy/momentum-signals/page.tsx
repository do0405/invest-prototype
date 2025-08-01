'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import SimpleDataTable from '@/components/SimpleDataTable';
import TradingViewChart from '@/components/TradingViewChart';
import { apiClient, ScreeningData } from '@/lib/api';

export default function MomentumSignalsPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getMomentumSignalsResults();
      if (res.success && res.data) {
        setData(res.data);
        setError(null);
      } else {
        setError(res.message || 'Failed to fetch data');
      }
      setLoading(false);
    };
    fetchData();
  }, []);

  // 간단한 컬럼 구성: 종목명과 시그널 발생일만 표시
  const simpleColumns = [
    {
      key: 'symbol',
      header: '종목명',
      render: (item: Record<string, unknown>) => (
        <span className="font-semibold text-blue-600">{String(item.symbol ?? 'N/A')}</span>
      )
    },
    {
      key: 'signal_date',
      header: '모멘텀 시그널 발생일',
      render: (item: Record<string, unknown>) => {
        const value = item.signal_date;
        if (value) {
          try {
            const date = new Date(value as string);
            return date.toLocaleDateString('ko-KR');
          } catch {
            return String(value);
          }
        }
        return 'N/A';
      }
    }
  ];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading momentum signals...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-red-600">{error}</div>
      </div>
    );
  }

  const handleRowClick = (item: Record<string, unknown>) => {
    setSelectedSymbol(item.symbol as string);
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <div className="flex items-center justify-between mb-4">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 mb-2">모멘텀 시그널 스크리너</h1>
            <p className="text-gray-600">Stan Weinstein Stage 2 Breakout 전략 기반 모멘텀 신호 탐지</p>
          </div>
          <Link
            href="/markminervini/all"
            className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          >
            전체 스크리너 보기
          </Link>
        </div>
        
        {/* 스크리닝 조건 설명 */}
        <div className="bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg border border-blue-200 p-6 mb-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-4 flex items-center space-x-2">
            <span>📋</span>
            <span>모멘텀 스크리닝 조건</span>
          </h2>
          
          <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
            <div>
              <h3 className="font-medium text-gray-700 mb-3">📊 기본 데이터 요구사항</h3>
              <ul className="space-y-2 text-sm text-gray-600">
                <li className="flex items-start space-x-2">
                  <span className="text-blue-500 mt-1">•</span>
                  <span>최소 200일 이상의 일간 OHLCV 데이터</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-blue-500 mt-1">•</span>
                  <span>주간 데이터로 변환하여 분석 (일 → 주 타임프레임)</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-blue-500 mt-1">•</span>
                  <span>RS Score 및 섹터 메타데이터</span>
                </li>
              </ul>
            </div>
            
            <div>
              <h3 className="font-medium text-gray-700 mb-3">🎯 핵심 필터링 조건</h3>
              <ul className="space-y-2 text-sm text-gray-600">
                <li className="flex items-start space-x-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span>현재가 > 30주 이동평균선</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span>30주 이동평균선 상승 추세</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span>거래량 비율 > 2.0 (20주 평균 대비)</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span>OBV(On-Balance Volume) 상승</span>
                </li>
                <li className="flex items-start space-x-2">
                  <span className="text-green-500 mt-1">•</span>
                  <span>최근 6주 내 패턴 형성</span>
                </li>
              </ul>
            </div>
          </div>
          
          <div className="mt-6 pt-4 border-t border-blue-200">
            <h3 className="font-medium text-gray-700 mb-3">🌍 시장 환경 조건</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div className="flex items-start space-x-2">
                <span className="text-purple-500 mt-1">•</span>
                <span className="text-sm text-gray-600">SPY > 150일 이동평균선</span>
              </div>
              <div className="flex items-start space-x-2">
                <span className="text-purple-500 mt-1">•</span>
                <span className="text-sm text-gray-600">SPY 150일 이동평균선 상승 추세</span>
              </div>
            </div>
          </div>
        </div>
      </div>
      
      <Link href="/" className="text-blue-500 hover:text-blue-700 mb-4 inline-block">
        ← Back to Dashboard
      </Link>
      
      {/* TradingView Chart */}
      {selectedSymbol && (
        <div className="mb-6">
          <div className="bg-white rounded-lg shadow p-4">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">
              {selectedSymbol} 차트
            </h2>
            <TradingViewChart symbol={selectedSymbol} height="500px" />
          </div>
        </div>
      )}
      
      {data.length > 0 ? (
        <SimpleDataTable 
          data={data} 
          columns={simpleColumns}
          title="모멘텀 시그널 종목"
          description={`총 ${data.length}개 종목에서 모멘텀 시그널 감지${selectedSymbol ? '' : ' (종목을 클릭하면 차트를 볼 수 있습니다)'}`}
          onRowClick={handleRowClick}
        />
      ) : (
        <div className="text-center text-gray-500">No data available</div>
      )}
    </div>
  );
}
