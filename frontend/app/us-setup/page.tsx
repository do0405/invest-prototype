'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import { apiClient, ScreeningData } from '@/lib/api';
import AlgorithmDescription from '@/components/AlgorithmDescription';

export default function USSetupPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const res = await apiClient.getUSSetupResults();
        if (res.success && res.data) {
          setData(res.data);
          setError(null);
        } else {
          setError(res.message || 'Failed to fetch data');
        }
      } catch (err) {
        console.error('Error fetching US Setup data:', err);
        setError('Network error occurred');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  const columns: DataTableColumn<ScreeningData>[] = [
    { key: 'symbol', header: 'Symbol' },
    { key: 'price', header: 'Price', align: 'right' }
  ];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading US Setup results...</div>
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
      <h1 className="text-3xl font-bold text-gray-800 mb-6">US Setup Screener</h1>
      <p className="text-gray-600 mb-6">
        미국 주식 중 기술적 셋업 조건을 만족하는 종목들을 선별한 결과입니다.
      </p>
      <AlgorithmDescription algorithm="us-setup" />
      {data.length > 0 ? (
        <DataTable data={data} columns={columns} />
      ) : (
        <div className="text-center text-gray-500">No data available</div>
      )}
    </div>
  );
}