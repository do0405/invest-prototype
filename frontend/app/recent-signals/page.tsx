'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import TradingViewChart from '@/components/TradingViewChart';
import { apiClient, ScreeningData } from '@/lib/api';

interface RecentSignal extends ScreeningData {
  screener: string;
  signal_date: string;
  price: number | string;
  change_pct: number | string;
  rs_score: number | string;
}

export default function RecentSignalsPage() {
  const [signals, setSignals] = useState<RecentSignal[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [daysFilter, setDaysFilter] = useState(5);

  const fetchRecentSignals = async (days: number = 5) => {
    try {
      setLoading(true);
      const result = await apiClient.getRecentSignals(days);
      
      if (result.success && result.data) {
        setSignals(result.data as RecentSignal[]);
        setError(null);
      } else {
        setError(result.message || 'Failed to fetch recent signals');
      }
    } catch (err) {
      console.error('Error fetching recent signals:', err);
      setError('Network error occurred');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRecentSignals(daysFilter);
  }, [daysFilter]);

  const getScreenerDisplayName = (screener: string) => {
    const names: { [key: string]: string } = {
      'momentum_signals': '모멘텀 시그널',
      'leader_stock': '리더 주식',
      'us_gainer': 'US 상승주',
      'us_setup': 'US 셋업',
      'markminervini': 'Mark Minervini',
      'volatility_skew': '변동성 스큐'
    };
    return names[screener] || screener;
  };

  const getScreenerColor = (screener: string) => {
    const colors: { [key: string]: string } = {
      'momentum_signals': 'bg-blue-100 text-blue-800',
      'leader_stock': 'bg-green-100 text-green-800',
      'us_gainer': 'bg-purple-100 text-purple-800',
      'us_setup': 'bg-yellow-100 text-yellow-800',
      'markminervini': 'bg-red-100 text-red-800',
      'volatility_skew': 'bg-indigo-100 text-indigo-800'
    };
    return colors[screener] || 'bg-gray-100 text-gray-800';
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading recent signals...</div>
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
            onClick={() => fetchRecentSignals(daysFilter)} 
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
        <h1 className="text-3xl font-bold text-gray-800">최근 시그널 포착 종목</h1>
        
        {/* 필터 옵션 */}
        <div className="flex items-center space-x-2">
          <label className="text-sm font-medium text-gray-700">필터:</label>
          <select 
            value={daysFilter} 
            onChange={(e) => setDaysFilter(Number(e.target.value))}
            className="border border-gray-300 rounded px-3 py-1 text-sm"
          >
            <option value={1}>1일 이내</option>
            <option value={3}>3일 이내</option>
            <option value={5}>5일 이내</option>
            <option value={7}>7일 이내</option>
            <option value={14}>14일 이내</option>
          </select>
        </div>
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

      <div className="mb-6 p-4 bg-blue-50 rounded-lg">
        <h2 className="text-lg font-semibold text-blue-800 mb-2">최근 시그널 분석</h2>
        <p className="text-blue-700">
          각 스크리너에서 독립적으로 감지된 최근 {daysFilter}일 이내의 시그널입니다. 
          스크리너별로 다른 시점에 감지되더라도 각각 신규 시그널로 처리됩니다.
          총 <span className="font-bold">{signals.length}개</span>의 최근 시그널이 발견되었습니다.
        </p>
      </div>

      {signals.length === 0 ? (
        <div className="text-center py-8">
          <p className="text-gray-500">최근 {daysFilter}일 이내에 감지된 시그널이 없습니다.</p>
        </div>
      ) : (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    스크리너
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    종목명
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    시그널 발생일
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    현재가
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    변화율
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    RS 점수
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {signals.map((signal, index) => (
                  <tr 
                    key={index} 
                    className="hover:bg-gray-50 cursor-pointer"
                    onClick={() => setSelectedSymbol(signal.symbol)}
                  >
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className={`inline-flex px-2 py-1 text-xs font-semibold rounded-full ${getScreenerColor(signal.screener)}`}>
                        {getScreenerDisplayName(signal.screener)}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      <span className="font-semibold text-blue-600">{signal.symbol}</span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {signal.signal_date}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {typeof signal.price === 'number' ? `$${signal.price.toFixed(2)}` : signal.price}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                      <span className={`${
                        typeof signal.change_pct === 'number' && signal.change_pct > 0 
                          ? 'text-green-600' 
                          : typeof signal.change_pct === 'number' && signal.change_pct < 0 
                          ? 'text-red-600' 
                          : 'text-gray-900'
                      }`}>
                        {typeof signal.change_pct === 'number' ? `${signal.change_pct.toFixed(2)}%` : signal.change_pct}
                      </span>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      {typeof signal.rs_score === 'number' ? signal.rs_score.toFixed(2) : signal.rs_score}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}