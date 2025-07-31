'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import { apiClient, ScreeningData } from '@/lib/api';

export default function QullamaggieParabolicShortPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getQullamaggieParabolicShort();
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

  const columns: DataTableColumn<ScreeningData>[] = data.length
    ? Object.keys(data[0]).slice(0, 8).map(key => ({
        key,
        header: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
      }))
    : [];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading parabolic short results...</div>
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
      <div className="mb-6">
        <h1 className="text-3xl font-bold text-gray-900 mb-2">
          📉 Qullamaggie Parabolic Short Strategy
        </h1>
        <p className="text-gray-600">
          Qullamaggie의 파라볼릭 숏 전략에 따른 스크리닝 결과입니다.
        </p>
        <div className="mt-4">
          <Link 
            href="/qullamaggie" 
            className="text-blue-600 hover:text-blue-800 underline"
          >
            ← Qullamaggie 메인으로 돌아가기
          </Link>
        </div>
      </div>
      
      <DataTable 
        data={data} 
        columns={columns}
        title="Parabolic Short Results"
        description={`총 ${data.length}개의 파라볼릭 숏 후보`}
      />
    </div>
  );
}