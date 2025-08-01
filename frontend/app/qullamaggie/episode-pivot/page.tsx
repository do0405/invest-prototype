'use client';
import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import NumberInputFilter, { NumberFilter } from '@/components/NumberInputFilter';
import { apiClient, ScreeningData } from '@/lib/api';

export default function QullamaggieEpisodePivotPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [numberFilters, setNumberFilters] = useState<NumberFilter[]>([]);
  const [showFilters, setShowFilters] = useState(false);
  const [searchTerm, setSearchTerm] = useState('');

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getQullamaggieEpisodePivot();
      if (res.success && res.data) {
        setData(res.data);
        initializeNumberFilters(res.data);
        setError(null);
      } else {
        setError(res.message || 'Failed to fetch data');
      }
      setLoading(false);
    };
    fetchData();
  }, []);

  const initializeNumberFilters = (data: ScreeningData[]) => {
    if (data.length === 0) return;
    
    const numericColumns = Object.keys(data[0] || {}).filter(key => {
      if (key.toLowerCase().includes('symbol') || key.toLowerCase().includes('ticker')) return false;
      
      const values = data.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val));
      return values.length > 0;
    });

    const filters: NumberFilter[] = numericColumns.map(key => {
      const values = data.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val)) as number[];
      const min = Math.min(...values);
      const max = Math.max(...values);
      const step = (max - min) > 100 ? Math.ceil((max - min) / 100) : 0.01;
      
      return {
        key,
        min,
        max,
        value: [min, max],
        step
      };
    });

    setNumberFilters(filters);
  };

  const handleNumberFilterChange = (filterKey: string, newValue: [number, number]) => {
    setNumberFilters(prev => 
      prev.map(filter => 
        filter.key === filterKey 
          ? { ...filter, value: newValue }
          : filter
      )
    );
  };

  const resetFilters = () => {
    setNumberFilters(prev => 
      prev.map(filter => ({
        ...filter,
        value: [filter.min, filter.max]
      }))
    );
    setSearchTerm('');
  };

  const toggleFilters = () => {
    setShowFilters(!showFilters);
  };

  const filteredData = useMemo(() => {
    return data.filter(item => {
      const searchMatch = !searchTerm || 
        item.symbol?.toString().toLowerCase().includes(searchTerm.toLowerCase());
      
      const numberMatch = numberFilters.every(filter => {
        const value = item[filter.key];
        if (typeof value !== 'number' || isNaN(value)) return true;
        return value >= filter.value[0] && value <= filter.value[1];
      });
      
      return searchMatch && numberMatch;
    });
  }, [data, searchTerm, numberFilters]);

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

      {/* 숫자 입력 필터 컴포넌트 */}
      <NumberInputFilter
        filters={numberFilters}
        onFilterChange={handleNumberFilterChange}
        onResetFilters={resetFilters}
        showFilters={showFilters}
        onToggleFilters={toggleFilters}
      />
      
      <DataTable 
        data={filteredData} 
        columns={columns}
        title="Episode Pivot Results"
        description={`총 ${filteredData.length}개의 에피소드 피벗 후보 (전체 ${data.length}개 중)`}
      />
    </div>
  );
}