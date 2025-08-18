'use client';
import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
// NumberInputFilter 제거됨 - 슬라이더 기반 필터 제거
import { apiClient, ScreeningData } from '@/lib/api';

export default function QullamaggieEpisodePivotPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  // 슬라이더 기반 필터 제거됨
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getQullamaggieEpisodePivot();
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

  // 슬라이더 기반 필터 초기화 제거됨

  const resetFilters = () => {
    setSearchTerm('');
  };

  const filteredData = useMemo(() => {
    return data.filter(item => {
      const searchMatch = !searchTerm || 
        item.symbol?.toString().toLowerCase().includes(searchTerm.toLowerCase());
      
      return searchMatch;
    });
  }, [data, searchTerm]);

  const columns: DataTableColumn<ScreeningData>[] = data.length
    ? Object.keys(data[0]).slice(0, 8).map(key => ({
        key,
        header: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
      }))
    : [];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading episode pivot results...</div>
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
          📈 Qullamaggie Episode Pivot Strategy
        </h1>
        <p className="text-gray-600">
          Qullamaggie의 에피소드 피벗 전략에 따른 스크리닝 결과입니다.
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

      {/* 검색 기능 */}
      <div className="mb-6">
        <div className="relative max-w-md">
          <input
            type="text"
            placeholder="종목 검색..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full px-4 py-2 pl-10 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-transparent"
          />
          <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none">
            <svg className="h-5 w-5 text-gray-400" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
            </svg>
          </div>
        </div>
      </div>


      
      <DataTable 
        data={filteredData} 
        columns={columns}
        title="Episode Pivot Results"
        description={`총 ${filteredData.length}개의 에피소드 피벗 후보 (전체 ${data.length}개 중)`}
      />
    </div>
  );
}