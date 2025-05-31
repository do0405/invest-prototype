'use client';

import { useEffect, useState, use } from 'react';
import { apiClient, PortfolioItem } from '@/lib/api';
import Link from 'next/link';

interface StrategyPageProps {
  params: Promise<{
    strategyId: string;
  }>;
}

export default function StrategyPage({ params }: StrategyPageProps) {
  const resolvedParams = use(params);
  const [data, setData] = useState<PortfolioItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchStrategyData = async () => {
      try {
        setLoading(true);
        const response = await apiClient.getPortfolioByStrategy(resolvedParams.strategyId);
        if (response.success && response.data) {
          setData(response.data);
          setError(null);
        } else {
          setError(response.message || 'Failed to fetch strategy data');
        }
      } catch (err) {
        console.error('Error fetching strategy data:', err);
        setError('Network error occurred');
      } finally {
        setLoading(false);
      }
    };

    if (resolvedParams.strategyId) {
      fetchStrategyData();
    }
  }, [resolvedParams.strategyId]);

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading strategy data...</div>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="container mx-auto px-4 py-8">
        <div className="text-center">
          <h1 className="text-3xl font-bold text-gray-800 mb-4">
            Strategy: {resolvedParams.strategyId}
          </h1>
          <div className="text-red-500 mb-4">
            <h2 className="text-xl font-bold mb-2">Error</h2>
            <p>{error}</p>
          </div>
          <div className="space-x-4">
            <button 
              onClick={() => window.location.reload()} 
              className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
            >
              Retry
            </button>
            <Link 
              href="/" 
              className="px-4 py-2 bg-gray-500 text-white rounded hover:bg-gray-600"
            >
              Back to Dashboard
            </Link>
          </div>
        </div>
      </div>
    );
  }

  const getStrategyType = (strategyId: string) => {
    return ['strategy2', 'strategy6'].includes(strategyId) ? 'sell' : 'buy';
  };

  const strategyType = getStrategyType(resolvedParams.strategyId);

  return (
    <div className="container mx-auto px-4 py-8 max-h-screen overflow-y-auto">
      <div className="mb-6">
        <Link 
          href="/" 
          className="text-blue-500 hover:text-blue-700 mb-4 inline-block"
        >
          ← Back to Dashboard
        </Link>
        <div className="flex items-center gap-4">
          <h1 className="text-3xl font-bold text-gray-800">
            Strategy: {resolvedParams.strategyId}
          </h1>
          <span className={`px-3 py-1 rounded-full text-sm font-medium ${
            strategyType === 'buy' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}>
            {strategyType === 'buy' ? '📈 Buy Strategy' : '📉 Sell Strategy'}
          </span>
        </div>
        <p className="text-gray-600 mt-2">
          {data.length} positions found
        </p>
      </div>
      
      {data.length > 0 ? (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="overflow-x-auto">
            <table className="min-w-full">
              <thead className={`${
                strategyType === 'buy' ? 'bg-green-50' : 'bg-red-50'
              }`}>
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    종목명
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    매수일
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    시장 진입가
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    비중(%)
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    수익률(%)
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    차익실현
                  </th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    손절매
                  </th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    수익보호
                  </th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase tracking-wider">
                    롱여부
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {data.map((item, index) => {
                  return (
                    <tr key={index} className="hover:bg-gray-50">
                      <td className="px-4 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                        {item.종목명 || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                        {item.매수일 || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-900 text-right">
                        {item['시장 진입가'] ? 
                          `$${typeof item['시장 진입가'] === 'number' ? item['시장 진입가'].toFixed(2) : item['시장 진입가']}` 
                          : 'N/A'
                        }
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500 text-right">
                        {item['비중(%)'] || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500 text-right">
                        {item['수익률(%)'] || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                        {item.차익실현 || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-red-600 text-right">
                        {item.손절매 ? 
                          `$${typeof item.손절매 === 'number' ? item.손절매.toFixed(2) : item.손절매}` 
                          : 'N/A'
                        }
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-sm text-gray-500">
                        {item.수익보호 || 'N/A'}
                      </td>
                      <td className="px-4 py-4 whitespace-nowrap text-center">
                        <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                          item.롱여부 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
                        }`}>
                          {item.롱여부 ? 'Long' : 'Short'}
                        </span>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      ) : (
        <div className="text-center py-12">
          <p className="text-gray-500 text-lg">No data available for this strategy.</p>
          <p className="text-gray-400 text-sm mt-2">
            The strategy "{resolvedParams.strategyId}" may not exist or has no positions.
          </p>
        </div>
      )}
    </div>
  );
}