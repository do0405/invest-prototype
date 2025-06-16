'use client';

import { useEffect, useState, use } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import { apiClient } from '@/lib/api';


interface ScreenerPageProps {
  params: Promise<{
    screenerId: string;
  }>;
}

interface ScreenerResult {
  symbol: string;
  [key: string]: any;
}

interface SliderFilter {
  key: string;
  min: number;
  max: number;
  value: [number, number];
  step: number;
}

export default function ScreenerPage({ params }: ScreenerPageProps) {
  const resolvedParams = use(params);
  const [data, setData] = useState<ScreenerResult[]>([]);
  const [filteredData, setFilteredData] = useState<ScreenerResult[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortConfig, setSortConfig] = useState<{ key: string; direction: 'asc' | 'desc' } | null>(null);
  const [sliderFilters, setSliderFilters] = useState<SliderFilter[]>([]);
  const [showFilters, setShowFilters] = useState(false);
  const [description, setDescription] = useState('');

  const getScreenerName = (id: string) => {
    const names: { [key: string]: string } = {
      'advanced_financial_results': 'Advanced Financial Results',
      'integrated_results': 'Integrated Results',
      'new_tickers': 'New Tickers',
      'previous_us_with_rs': 'Previous US with RS',
      'us_with_rs': 'US with RS',
      'pattern_analysis_results': 'Pattern Analysis Results'
    };
    return names[id] || id;
  };

  useEffect(() => {
    const fetchScreenerData = async () => {
      try {
        setLoading(true);
        const response = await fetch(`/api/markminervini/${resolvedParams.screenerId}`);
        if (response.ok) {
          const result = await response.json();
          if (result.success && result.data) {
            const dataArray = Array.isArray(result.data) ? result.data : [];
            setData(dataArray);
            initializeSliderFilters(dataArray);
            setError(null);
          } else {
            setError(result.error || 'Failed to fetch screener data');
          }
        } else {
          setError('Failed to fetch screener data');
        }
      } catch (err) {
        console.error('Error fetching screener data:', err);
        setError('Network error occurred');
      } finally {
        setLoading(false);
      }
    };
    const fetchDescription = async () => {
      const res = await apiClient.getScreenerDescription(resolvedParams.screenerId);
      if (res.success && res.data) {
        setDescription(res.data as unknown as string);
      }
    };

    fetchScreenerData();
    fetchDescription();
  }, [resolvedParams.screenerId]);

  const initializeSliderFilters = (dataArray: ScreenerResult[]) => {
    if (dataArray.length === 0) return;
    
    const numericColumns = Object.keys(dataArray[0]).filter(key => {
      // Symbol/ticker 컬럼 제외
      if (key.toLowerCase().includes('symbol') || key.toLowerCase().includes('ticker')) return false;
      
      // 숫자 컬럼만 선택
      const values = dataArray.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val));
      return values.length > 0;
    });

    const filters: SliderFilter[] = numericColumns.map(key => {
      const values = dataArray.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val));
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

    setSliderFilters(filters);
  };

  // 데이터 필터링 및 정렬
  useEffect(() => {
    let filtered = data.filter(item => {
      // 슬라이더 필터링
      return sliderFilters.every(filter => {
        const value = item[filter.key];
        if (typeof value !== 'number' || isNaN(value)) return true;
        return value >= filter.value[0] && value <= filter.value[1];
      });
    });

    // 정렬
    if (sortConfig) {
      filtered.sort((a, b) => {
        const aValue = a[sortConfig.key];
        const bValue = b[sortConfig.key];
        
        if (typeof aValue === 'number' && typeof bValue === 'number') {
          return sortConfig.direction === 'asc' ? aValue - bValue : bValue - aValue;
        }
        
        const aStr = String(aValue).toLowerCase();
        const bStr = String(bValue).toLowerCase();
        
        if (sortConfig.direction === 'asc') {
          return aStr < bStr ? -1 : aStr > bStr ? 1 : 0;
        } else {
          return aStr > bStr ? -1 : aStr < bStr ? 1 : 0;
        }
      });
    }

    setFilteredData(filtered);
  }, [data, sliderFilters, sortConfig]);

  const getTableHeaders = () => {
    if (data.length === 0) return [];
    
    // Symbol/ticker를 가장 먼저, True 값 컬럼 제외
    const allKeys = Object.keys(data[0]);
    const symbolKey = allKeys.find(key => 
      key.toLowerCase().includes('symbol') || key.toLowerCase().includes('ticker')
    ) || 'symbol';
    
    const otherKeys = allKeys
      .filter(key => key !== symbolKey)
      .filter(key => {
        // True 값을 가진 컬럼들을 필터링해서 제외
        const allTrue = data.every(item => item[key] === true || item[key] === 'True');
        return !allTrue;
      });
    
    return [symbolKey, ...otherKeys];
  };

  const handleSort = (key: string) => {
    setSortConfig(current => {
      if (current?.key === key) {
        return current.direction === 'asc' 
          ? { key, direction: 'desc' }
          : null;
      }
      return { key, direction: 'asc' };
    });
  };

  const handleSliderChange = (filterKey: string, newValue: [number, number]) => {
    setSliderFilters(prev => 
      prev.map(filter => 
        filter.key === filterKey 
          ? { ...filter, value: newValue }
          : filter
      )
    );
  };

  const resetFilters = () => {
    setSliderFilters(prev => 
      prev.map(filter => ({
        ...filter,
        value: [filter.min, filter.max]
      }))
    );
    setSortConfig(null);
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading screener data...</div>
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

  const headers = getTableHeaders();
  const columns: DataTableColumn<ScreenerResult>[] = headers.map((header) => ({
    key: header,
    header: header.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
    render: (item) => {
      const value = item[header];
      if (header.toLowerCase().includes('symbol') || header.toLowerCase().includes('ticker')) {
        return <span className="font-semibold text-purple-600">{String(value ?? 'N/A')}</span>;
      }
      return typeof value === 'number' ? value.toFixed(2) : String(value ?? 'N/A');
    },
  }));

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
            {getScreenerName(resolvedParams.screenerId)}
          </h1>
          <span className="px-3 py-1 rounded-full text-sm font-medium bg-purple-100 text-purple-800">
            🔍 Markminervini Screener
          </span>
        </div>
        <p className="text-gray-600 mt-2">
          {filteredData.length} of {data.length} results
        </p>
        {description && (
          <pre className="whitespace-pre-wrap bg-gray-50 p-4 mt-4 rounded text-sm">
            {description}
          </pre>
        )}
      </div>
      
      {/* 필터 토글 버튼 */}
      <div className="mb-6">
        <button
          onClick={() => setShowFilters(!showFilters)}
          className="px-6 py-3 bg-gradient-to-r from-purple-600 to-blue-600 text-white rounded-lg hover:from-purple-700 hover:to-blue-700 transition-all duration-300 shadow-lg hover:shadow-xl transform hover:scale-105 flex items-center gap-2"
        >
          <span className="text-lg">🎛️</span>
          {showFilters ? 'Hide Filters' : 'Show Filters'}
          <span className={`transform transition-transform duration-300 ${showFilters ? 'rotate-180' : ''}`}>
            ▼
          </span>
        </button>
      </div>

      {/* 슬라이더 필터 패널 */}
      <div className={`overflow-hidden transition-all duration-500 ease-in-out ${
        showFilters 
          ? 'max-h-96 opacity-100 transform translate-y-0' 
          : 'max-h-0 opacity-0 transform -translate-y-4'
      }`}>
        <div className="bg-gradient-to-br from-white to-gray-50 rounded-xl shadow-lg p-6 mb-6 border border-gray-200">
          <div className="flex justify-between items-center mb-4">
            <h3 className="text-lg font-semibold text-gray-800 flex items-center gap-2">
              <span>🎯</span>
              Filter Controls
            </h3>
            <button
              onClick={resetFilters}
              className="px-4 py-2 bg-gray-500 text-white rounded-md hover:bg-gray-600 transition-colors"
            >
              Reset All
            </button>
          </div>
          
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {sliderFilters.map((filter) => (
              <div key={filter.key} className="space-y-3">
                <label className="block text-sm font-medium text-gray-700">
                  {filter.key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase())}
                </label>
                <div className="px-3 py-2 bg-white rounded-lg border border-gray-200 shadow-sm">
                  <div className="flex justify-between text-xs text-gray-500 mb-2">
                    <span>{filter.value[0].toFixed(2)}</span>
                    <span>{filter.value[1].toFixed(2)}</span>
                  </div>
                  <input
                    type="range"
                    min={filter.min}
                    max={filter.max}
                    step={filter.step}
                    value={filter.value[0]}
                    onChange={(e) => handleSliderChange(filter.key, [parseFloat(e.target.value), filter.value[1]])}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider-thumb"
                  />
                  <input
                    type="range"
                    min={filter.min}
                    max={filter.max}
                    step={filter.step}
                    value={filter.value[1]}
                    onChange={(e) => handleSliderChange(filter.key, [filter.value[0], parseFloat(e.target.value)])}
                    className="w-full h-2 bg-gray-200 rounded-lg appearance-none cursor-pointer slider-thumb mt-1"
                  />
                  <div className="flex justify-between text-xs text-gray-400 mt-1">
                    <span>Min: {filter.min.toFixed(2)}</span>
                    <span>Max: {filter.max.toFixed(2)}</span>
                  </div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
      
      {filteredData.length > 0 ? (
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <DataTable
            data={filteredData}
            columns={columns}
            headerRowClassName="bg-purple-50"
          />
        </div>
      ) : (
        <div className="text-center py-12">
          <p className="text-gray-500 text-lg">No data matches your filters.</p>
          <p className="text-gray-400 text-sm mt-2">
            Try adjusting your slider ranges or resetting filters.
          </p>
        </div>
      )}
      
      <style jsx>{`
        .slider-thumb::-webkit-slider-thumb {
          appearance: none;
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          cursor: pointer;
          box-shadow: 0 2px 6px rgba(0,0,0,0.2);
          transition: all 0.2s ease;
        }
        
        .slider-thumb::-webkit-slider-thumb:hover {
          transform: scale(1.1);
          box-shadow: 0 4px 12px rgba(0,0,0,0.3);
        }
        
        .slider-thumb::-moz-range-thumb {
          height: 20px;
          width: 20px;
          border-radius: 50%;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          cursor: pointer;
          border: none;
          box-shadow: 0 2px 6px rgba(0,0,0,0.2);
        }
      `}</style>
    </div>
  );
}