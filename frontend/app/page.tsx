'use client';

import { useEffect, useState } from 'react';
import { apiClient, SummaryData } from '@/lib/api';
import Link from 'next/link';

export default function HomePage() {
  const [summary, setSummary] = useState<SummaryData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchSummary = async () => {
      try {
        setLoading(true);
        const response = await apiClient.getSummary();
        if (response.success && response.data) {
          setSummary(response.data);
          setError(null);
        } else {
          setError(response.message || 'Failed to fetch data');
        }
      } catch (err) {
        console.error('Error fetching summary:', err);
        setError('Network error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchSummary();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading...</div>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-red-500">
          <h2 className="text-xl font-bold mb-2">Error</h2>
          <p>{error}</p>
          <button 
            onClick={() => window.location.reload()} 
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
      <h1 className="text-3xl font-bold text-gray-800 mb-8">Investment Dashboard</h1>
      
      {summary && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {/* 기술적 스크리닝 */}
          {summary.technical_screening && (
            <Link href="/screening/technical" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-blue-600">Technical Screening</h2>
                <p className="text-gray-600">Total: {summary.technical_screening.count} stocks</p>
                <p className="text-sm text-gray-500 mt-2">Click to view details</p>
              </div>
            </Link>
          )}
          
          {/* 재무제표 스크리닝 */}
          {summary.financial_screening && (
            <Link href="/screening/financial" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-green-600">Financial Screening</h2>
                <p className="text-gray-600">Total: {summary.financial_screening.count} stocks</p>
                <p className="text-sm text-gray-500 mt-2">Click to view details</p>
              </div>
            </Link>
          )}
          
          {/* 통합 스크리닝 */}
          {summary.integrated_screening && (
            <Link href="/screening/integrated" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-purple-600">Integrated Screening</h2>
                <p className="text-gray-600">Total: {summary.integrated_screening.count} stocks</p>
                <p className="text-sm text-gray-500 mt-2">Click to view details</p>
              </div>
            </Link>
          )}
          
          {/* 전략별 현황 */}
          {Object.entries(summary.strategies).map(([strategy, data]) => (
            <Link key={strategy} href={`/strategy/${strategy}`} className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-orange-600">{strategy}</h2>
                <p className="text-gray-600">Positions: {data.active_positions}</p>
                <p className="text-gray-600">Total: {data.count} stocks</p>
                <p className="text-sm text-gray-500 mt-2">Click to view portfolio</p>
              </div>
            </Link>
          ))}
        </div>
      )}
      
      {!summary && !loading && !error && (
        <div className="text-center text-gray-500">
          <p>No data available</p>
        </div>
      )}
    </div>
  );
}