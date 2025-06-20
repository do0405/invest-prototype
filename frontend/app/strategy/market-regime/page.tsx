'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import { apiClient } from '@/lib/api';

interface RegimeData {
  date: string;
  score: number;
  regime_name: string;
  description: string;
  strategy: string;
}

export default function MarketRegimePage() {
  const [data, setData] = useState<RegimeData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getMarketRegime();
      if (res.success && res.data) {
        setData(res.data as unknown as RegimeData);
        setError(null);
      } else {
        setError(res.message || 'Failed to fetch data');
      }
      setLoading(false);
    };
    fetchData();
  }, []);

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading market regime...</div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-red-600">{error || 'No data'}</div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8">
      <Link href="/" className="text-blue-500 hover:text-blue-700 mb-4 inline-block">
        ← Back to Dashboard
      </Link>
      <h1 className="text-3xl font-bold text-gray-800 mb-6">Market Regime</h1>
      <div className="bg-white shadow rounded-lg p-6 space-y-4">
        <div className="text-gray-600">Date: {data.date}</div>
        <div className="text-lg font-semibold">Score: {data.score}</div>
        <div className="text-xl font-bold">{data.regime_name}</div>
        <p className="text-gray-700">{data.description}</p>
        <p className="text-gray-700">{data.strategy}</p>
      </div>
    </div>
  );
}
