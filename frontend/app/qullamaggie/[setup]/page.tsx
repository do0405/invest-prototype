'use client';
import { use, useEffect, useState } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import { apiClient, ScreeningData, ApiResponse } from '@/lib/api';

const fetcherMap: Record<string, () => Promise<ApiResponse<ScreeningData[]>>> = {
  breakout: apiClient.getQullamaggieBreakout,
  'episode-pivot': apiClient.getQullamaggieEpisodePivot,
  'parabolic-short': apiClient.getQullamaggieParabolicShort,
  'buy-signals': apiClient.getQullamaggieBuySignals,
  'sell-signals': apiClient.getQullamaggieSellSignals,
};

interface PageProps {
  params: Promise<{ setup: string }>;
}

export default function QullamaggieSetupPage({ params }: PageProps) {
  const { setup } = use(params);
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const fetcher = fetcherMap[setup];
      if (!fetcher) {
        setError('Unknown setup');
        setLoading(false);
        return;
      }
      const res = await fetcher.call(apiClient);
      if (res.success && res.data) {
        setData(res.data);
        setError(null);
      } else {
        setError(res.message || 'Failed to fetch data');
      }
      setLoading(false);
    };
    fetchData();
  }, [setup]);

  const columns: DataTableColumn<ScreeningData>[] = data.length
    ? Object.keys(data[0]).slice(0, 8).map(key => ({
        key,
        header: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
      }))
    : [];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading {setup} results...</div>
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

  return (
    <div className="container mx-auto px-4 py-8">
      <Link href="/" className="text-blue-500 hover:text-blue-700 mb-4 inline-block">
        ← Back to Dashboard
      </Link>
      <h1 className="text-3xl font-bold text-gray-800 mb-6">Qullamaggie {setup.replace('-', ' ')}</h1>
      {data.length > 0 ? (
        <DataTable data={data} columns={columns} />
      ) : (
        <div className="text-center text-gray-500">No data available</div>
      )}
    </div>
  );
}
