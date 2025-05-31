'use client';
import { useEffect, useState } from 'react';
import { apiClient, PortfolioItem } from '@/lib/api';
import Link from 'next/link';

interface StrategyData {
  strategyId: string;
  strategyName: string;
  data: PortfolioItem[];
  type: 'buy' | 'sell';
}

export default function AllStrategiesPage() {
  const [strategiesData, setStrategiesData] = useState<StrategyData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const strategies = [
    { id: 'strategy1', name: 'Strategy 1', type: 'buy' as const },
    { id: 'strategy2', name: 'Strategy 2', type: 'sell' as const },
    { id: 'strategy3', name: 'Strategy 3', type: 'buy' as const },
    { id: 'strategy4', name: 'Strategy 4', type: 'buy' as const },
    { id: 'strategy5', name: 'Strategy 5', type: 'buy' as const },
    { id: 'strategy6', name: 'Strategy 6', type: 'sell' as const },
  ];

  useEffect(() => {
    const fetchAllStrategies = async () => {
      try {
        setLoading(true);
        const results: StrategyData[] = [];
        
        for (const strategy of strategies) {
          try {
            const response = await apiClient.getPortfolioByStrategy(strategy.id);
            if (response.success && response.data) {
              results.push({
                strategyId: strategy.id,
                strategyName: strategy.name,
                data: response.data,
                type: strategy.type
              });
            }
          } catch (err) {
            console.warn(`Failed to fetch ${strategy.id}:`, err);
          }
        }
        
        setStrategiesData(results);
        setError(null);
      } catch (err) {
        console.error('Error fetching strategies:', err);
        setError('Failed to fetch strategy data');
      } finally {
        setLoading(false);
      }
    };

    fetchAllStrategies();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading all strategies...</div>
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

  const buyStrategies = strategiesData.filter(s => s.type === 'buy');
  const sellStrategies = strategiesData.filter(s => s.type === 'sell');

  return (
    <div className="container mx-auto px-4 py-8">
      <div className="mb-8">
        <Link href="/" className="text-blue-600 hover:text-blue-800 mb-4 inline-block">
          ← Back to Home
        </Link>
        <h1 className="text-3xl font-bold text-gray-900 mb-2">All Strategies Overview</h1>
        <p className="text-gray-600">Complete view of all Strategy Alpha components (Strategy 1-6)</p>
      </div>

      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <div className="bg-green-50 border border-green-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-green-800 mb-2">📈 Buy Strategies</h3>
          <p className="text-2xl font-bold text-green-600">{buyStrategies.length}</p>
          <p className="text-sm text-green-600">Active buy strategies</p>
        </div>
        <div className="bg-red-50 border border-red-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-red-800 mb-2">📉 Sell Strategies</h3>
          <p className="text-2xl font-bold text-red-600">{sellStrategies.length}</p>
          <p className="text-sm text-red-600">Active sell strategies</p>
        </div>
        <div className="bg-blue-50 border border-blue-200 rounded-lg p-6">
          <h3 className="text-lg font-semibold text-blue-800 mb-2">📊 Total Positions</h3>
          <p className="text-2xl font-bold text-blue-600">
            {strategiesData.reduce((sum, s) => sum + s.data.length, 0)}
          </p>
          <p className="text-sm text-blue-600">Total portfolio positions</p>
        </div>
      </div>

      {/* Buy Strategies Section */}
      {buyStrategies.length > 0 && (
        <div className="mb-12">
          <h2 className="text-2xl font-bold text-green-800 mb-6 flex items-center">
            📈 Buy Strategies
          </h2>
          <div className="space-y-8">
            {buyStrategies.map((strategy) => (
              <div key={strategy.strategyId} className="bg-white border border-green-200 rounded-lg shadow-sm">
                <div className="bg-green-50 px-6 py-4 border-b border-green-200">
                  <div className="flex justify-between items-center">
                    <h3 className="text-xl font-semibold text-green-800">{strategy.strategyName}</h3>
                    <div className="flex items-center space-x-4">
                      <span className="text-sm text-green-600">{strategy.data.length} positions</span>
                      <Link 
                        href={`/strategy/${strategy.strategyId}`}
                        className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                      >
                        View Details →
                      </Link>
                    </div>
                  </div>
                </div>
                <div className="p-6">
                  {strategy.data.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="min-w-full">
                        <thead>
                          <tr className="border-b border-gray-200">
                            <th className="text-left py-2 px-3 font-medium text-gray-700">종목명</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">매수일</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">시장 진입가</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">비중(%)</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">수익률(%)</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">차익실현</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">손절매</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">수익보호</th>
                            <th className="text-center py-2 px-3 font-medium text-gray-700">롱여부</th>
                          </tr>
                        </thead>
                        <tbody>
                          {strategy.data.map((item, index) => (
                            <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                              <td className="py-2 px-3 font-medium text-gray-900">{item.종목명}</td>
                              <td className="py-2 px-3 text-gray-600">{item.매수일}</td>
                              <td className="py-2 px-3 text-right text-gray-900">
                                ${typeof item['시장 진입가'] === 'number' ? item['시장 진입가'].toFixed(2) : item['시장 진입가']}
                              </td>
                              <td className="py-2 px-3 text-right text-gray-600">{item['비중(%)']}</td>
                              <td className="py-2 px-3 text-right text-gray-600">{item['수익률(%)']}</td>
                              <td className="py-2 px-3 text-gray-600">{item.차익실현}</td>
                              <td className="py-2 px-3 text-right text-red-600">
                                ${typeof item.손절매 === 'number' ? item.손절매.toFixed(2) : item.손절매}
                              </td>
                              <td className="py-2 px-3 text-gray-600">{item.수익보호}</td>
                              <td className="py-2 px-3 text-center">
                                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                  item.롱여부 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                                }`}>
                                  {item.롱여부 ? 'Long' : 'Short'}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-gray-500 text-center py-4">No positions available</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Sell Strategies Section */}
      {sellStrategies.length > 0 && (
        <div className="mb-12">
          <h2 className="text-2xl font-bold text-red-800 mb-6 flex items-center">
            📉 Sell Strategies
          </h2>
          <div className="space-y-8">
            {sellStrategies.map((strategy) => (
              <div key={strategy.strategyId} className="bg-white border border-red-200 rounded-lg shadow-sm">
                <div className="bg-red-50 px-6 py-4 border-b border-red-200">
                  <div className="flex justify-between items-center">
                    <h3 className="text-xl font-semibold text-red-800">{strategy.strategyName}</h3>
                    <div className="flex items-center space-x-4">
                      <span className="text-sm text-red-600">{strategy.data.length} positions</span>
                      <Link 
                        href={`/strategy/${strategy.strategyId}`}
                        className="text-blue-600 hover:text-blue-800 text-sm font-medium"
                      >
                        View Details →
                      </Link>
                    </div>
                  </div>
                </div>
                <div className="p-6">
                  {strategy.data.length > 0 ? (
                    <div className="overflow-x-auto">
                      <table className="min-w-full">
                        <thead>
                          <tr className="border-b border-gray-200">
                            <th className="text-left py-2 px-3 font-medium text-gray-700">종목명</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">매수일</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">시장 진입가</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">비중(%)</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">수익률(%)</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">차익실현</th>
                            <th className="text-right py-2 px-3 font-medium text-gray-700">손절매</th>
                            <th className="text-left py-2 px-3 font-medium text-gray-700">수익보호</th>
                            <th className="text-center py-2 px-3 font-medium text-gray-700">롱여부</th>
                          </tr>
                        </thead>
                        <tbody>
                          {strategy.data.map((item, index) => (
                            <tr key={index} className="border-b border-gray-100 hover:bg-gray-50">
                              <td className="py-2 px-3 font-medium text-gray-900">{item.종목명}</td>
                              <td className="py-2 px-3 text-gray-600">{item.매수일}</td>
                              <td className="py-2 px-3 text-right text-gray-900">
                                ${typeof item['시장 진입가'] === 'number' ? item['시장 진입가'].toFixed(2) : item['시장 진입가']}
                              </td>
                              <td className="py-2 px-3 text-right text-gray-600">{item['비중(%)']}</td>
                              <td className="py-2 px-3 text-right text-gray-600">{item['수익률(%)']}</td>
                              <td className="py-2 px-3 text-gray-600">{item.차익실현}</td>
                              <td className="py-2 px-3 text-right text-red-600">
                                ${typeof item.손절매 === 'number' ? item.손절매.toFixed(2) : item.손절매}
                              </td>
                              <td className="py-2 px-3 text-gray-600">{item.수익보호}</td>
                              <td className="py-2 px-3 text-center">
                                <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                                  item.롱여부 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                                }`}>
                                  {item.롱여부 ? 'Long' : 'Short'}
                                </span>
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  ) : (
                    <p className="text-gray-500 text-center py-4">No positions available</p>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}