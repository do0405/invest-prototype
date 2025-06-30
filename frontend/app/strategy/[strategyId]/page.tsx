'use client';

import { useEffect, useState, use } from 'react';
import { apiClient, PortfolioItem } from '@/lib/api';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';

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
  const [description, setDescription] = useState('');

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
    const fetchDescription = async () => {
      const res = await apiClient.getStrategyDescription(resolvedParams.strategyId);
      if (res.success && res.data) {
        setDescription(res.data as unknown as string);
      }
    };

    if (resolvedParams.strategyId) {
      fetchStrategyData();
      fetchDescription();
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

  const columns: DataTableColumn<PortfolioItem>[] = [
    { key: '종목명', header: '종목명' },
    { key: '매수일', header: '매수일' },
    {
      key: '시장 진입가',
      header: '시장 진입가',
      align: 'right',
      render: (item) =>
        item['시장 진입가']
          ? `$${
              typeof item['시장 진입가'] === 'number'
                ? item['시장 진입가'].toFixed(2)
                : item['시장 진입가']
            }`
          : 'N/A',
    },
    { key: '비중(%)', header: '비중(%)', align: 'right' },
    { key: '수익률(%)', header: '수익률(%)', align: 'right' },
    { key: '차익실현', header: '차익실현' },
    {
      key: '손절매',
      header: '손절매',
      align: 'right',
      render: (item) =>
        item.손절매
          ? `$${
              typeof item.손절매 === 'number'
                ? item.손절매.toFixed(2)
                : item.손절매
            }`
          : 'N/A',
    },
    { key: '수익보호', header: '수익보호' },
    {
      key: '롱여부',
      header: '롱여부',
      align: 'center',
      render: (item) => (
        <span
          className={`px-2 py-1 rounded-full text-xs font-medium ${
            item.롱여부 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'
          }`}
        >
          {item.롱여부 ? 'Long' : 'Short'}
        </span>
      ),
    },
  ];

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
        {description && (
          <pre className="whitespace-pre-wrap bg-gray-50 p-4 mt-4 rounded text-sm">
            {description}
          </pre>
        )}
      </div>
      
      {data.length > 0 ? (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <DataTable
            data={data}
            columns={columns}
            headerRowClassName={strategyType === 'buy' ? 'bg-green-50' : 'bg-red-50'}
          />
        </div>
      ) : (
        <div className="text-center py-12">
          <p className="text-gray-500 text-lg">No data available for this strategy.</p>
          <p className="text-gray-400 text-sm mt-2">
            The strategy &quot;{resolvedParams.strategyId}&quot; may not exist or has no positions.
          </p>
        </div>
      )}
    </div>
  );
}
