'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import TradingViewChart from '@/components/TradingViewChart';
import { apiClient, ScreeningData } from '@/lib/api';

interface TopStock extends ScreeningData {
  rank: number;
  topsis_score: number;
  rs_score: number;
  price_momentum_20d: number;
  // Pattern detection fields for compatibility
  vcp_detected?: boolean;
  VCP_Pattern?: boolean;
  cup_handle_detected?: boolean;
  Cup_Handle_Pattern?: boolean;
}

export default function TopRecommendationsPage() {
  const [topStocks, setTopStocks] = useState<TopStock[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);

  const fetchTopStocks = async () => {
    try {
      setLoading(true);
      const result = await apiClient.getTopStocks();
      
      if (result.success && result.data) {
        setTopStocks(result.data as TopStock[]);
        setLastUpdated(result.last_updated || null);
        setError(null);
      } else {
        setError(result.message || 'Failed to fetch top stocks');
      }
    } catch (err) {
      console.error('Error fetching top stocks:', err);
      setError('Network error occurred');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchTopStocks();
  }, []);

  const getScoreColor = (score: number) => {
    if (score > 0.8) return 'text-green-600 font-bold';
    if (score > 0.6) return 'text-blue-600 font-semibold';
    if (score > 0.4) return 'text-yellow-600';
    return 'text-red-600';
  };

  const getMomentumColor = (momentum: number) => {
    if (momentum > 10) return 'text-green-600';
    if (momentum > 0) return 'text-blue-600';
    return 'text-red-600';
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading top recommendations...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-red-600">
          <h2 className="text-xl font-bold mb-2">Error</h2>
          <p>{error}</p>
          <button 
            onClick={fetchTopStocks} 
            className="mt-4 px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <Link href="/" className="text-blue-500 hover:text-blue-700 mb-4 inline-block">
        ← Back to Dashboard
      </Link>
      
      <div className="flex justify-between items-center mb-6">
        <h1 className="text-3xl font-bold text-gray-800">매수 랭킹 Top 10</h1>
        {lastUpdated && (
          <div className="text-sm text-gray-500">
            마지막 업데이트: {new Date(lastUpdated).toLocaleString('ko-KR')}
          </div>
        )}
      </div>

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

      <div className="mb-6 p-4 bg-green-50 rounded-lg">
        <h2 className="text-lg font-semibold text-green-800 mb-2">TOPSIS 기반 종합 랭킹</h2>
        <p className="text-green-700">
          다중 기준 의사결정 분석(MCDA) 방법론을 사용하여 기술적 지표, 재무 지표, 상대 강도를 종합적으로 평가한 
          매수 추천 상위 10개 종목입니다. 높은 점수일수록 투자 매력도가 높습니다.
        </p>
      </div>

      {topStocks.length === 0 ? (
        <div className="text-center py-8">
          <p className="text-gray-500">랭킹 데이터가 없습니다.</p>
        </div>
      ) : (
        <div className="space-y-4">
          {/* 상위 3개 하이라이트 */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
            {topStocks.slice(0, 3).map((stock, index) => (
              <div 
                key={stock.symbol} 
                className={`p-6 rounded-lg shadow-lg cursor-pointer transform hover:scale-105 transition-transform ${
                  index === 0 ? 'bg-gradient-to-r from-yellow-400 to-yellow-500 text-white' :
                  index === 1 ? 'bg-gradient-to-r from-gray-300 to-gray-400 text-white' :
                  'bg-gradient-to-r from-orange-300 to-orange-400 text-white'
                }`}
                onClick={() => setSelectedSymbol(stock.symbol)}
              >
                <div className="flex items-center justify-between mb-2">
                  <span className="text-2xl font-bold">#{stock.rank}</span>
                  <span className="text-lg font-semibold">{stock.symbol}</span>
                </div>
                <div className="text-sm opacity-90">
                  <div>TOPSIS 점수: {stock.topsis_score.toFixed(4)}</div>
                  <div>RS 점수: {stock.rs_score.toFixed(1)}</div>
                  <div>20일 모멘텀: {stock.price_momentum_20d.toFixed(1)}%</div>
                </div>
              </div>
            ))}
          </div>

          {/* 전체 테이블 */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      순위
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      종목명
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      TOPSIS 점수
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      RS 점수
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                      20일 모멘텀
                    </th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {topStocks.map((stock) => (
                    <tr 
                      key={stock.symbol} 
                      className="hover:bg-gray-50 cursor-pointer"
                      onClick={() => setSelectedSymbol(stock.symbol)}
                    >
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex items-center justify-center w-8 h-8 rounded-full text-white font-bold ${
                          stock.rank <= 3 ? 'bg-yellow-500' :
                          stock.rank <= 5 ? 'bg-blue-500' :
                          'bg-gray-500'
                        }`}>
                          {stock.rank}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className="font-semibold text-blue-600 text-lg">{stock.symbol}</span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={getScoreColor(stock.topsis_score)}>
                          {stock.topsis_score.toFixed(4)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={stock.rs_score > 80 ? 'text-green-600' : stock.rs_score > 60 ? 'text-blue-600' : 'text-gray-900'}>
                          {stock.rs_score.toFixed(1)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={getMomentumColor(stock.price_momentum_20d)}>
                          {stock.price_momentum_20d.toFixed(1)}%
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* 지표 설명 */}
          <div className="mt-6 p-4 bg-gray-50 rounded-lg">
            <h3 className="text-lg font-semibold text-gray-800 mb-3">지표 설명</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm text-gray-700">
              <div>
                <strong>TOPSIS 점수:</strong> 다중 기준 종합 평가 점수 (높을수록 좋음)
              </div>
              <div>
                <strong>20일 모멘텀:</strong> 최근 20일간 가격 상승률
              </div>
              <div>
                <strong>RSI (14):</strong> 상대강도지수 (30 이하: 과매도, 70 이상: 과매수)
              </div>
              <div>
                <strong>P/E 비율:</strong> 주가수익비율 (낮을수록 저평가)
              </div>
              <div>
                <strong>ROE:</strong> 자기자본이익률 (높을수록 좋음)
              </div>
              <div>
                <strong>RS 점수:</strong> 상대 강도 점수 (시장 대비 성과)
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}