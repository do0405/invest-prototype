'use client';
import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import SimpleDataTable from '@/components/SimpleDataTable';
import TradingViewChart from '@/components/TradingViewChart';
import ScreeningSummary from '@/components/ScreeningSummary';
interface ScreenerResult {
  symbol: string;
  [key: string]: string | number | boolean | null | undefined;
}

interface ScreenerData {
  name: string;
  data: ScreenerResult[];
  type: string;
  lastUpdated?: string;
}

interface SliderFilter {
  key: string;
  min: number;
  max: number;
  value: [number, number];
  step: number;
}

export default function AllMarkminerviniPage() {
  const [screenersData, setScreenersData] = useState<ScreenerData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [expandedScreeners, setExpandedScreeners] = useState<Set<string>>(new Set());
  const [sliderFilters, setSliderFilters] = useState<SliderFilter[]>([]);
  const [showFilters, setShowFilters] = useState(false);

  const screeners = useMemo(() => [
    { id: 'advanced_financial_results', name: 'Advanced Financial Results', icon: '💰' },
    { id: 'integrated_results', name: 'Integrated Results', icon: '🔗' },
    { id: 'new_tickers', name: 'New Tickers', icon: '🆕' },
    { id: 'previous_us_with_rs', name: 'Previous US with RS', icon: '📈' },
    { id: 'us_setup_results', name: 'US Setup Results', icon: '⚙️' },
    { id: 'us_gainers_results', name: 'US Gainers Results', icon: '📈' },
    { id: 'pattern_detection_results', name: 'Pattern Detection', icon: '📊' },
  ], []);

  useEffect(() => {
    const fetchAllScreeners = async () => {
      try {
        setLoading(true);
        const results: ScreenerData[] = [];
        
        for (const screener of screeners) {
          try {
            const response = await fetch(`/api/markminervini/${screener.id}`);
            if (response.ok) {
              const result = await response.json();
              console.log(`API response for ${screener.id}:`, result); // ADDED LOG
              if (result.success && result.data) {
                results.push({
                  name: screener.name,
                  data: Array.isArray(result.data) ? result.data : [],
                  type: screener.id,
                  lastUpdated: result.last_updated || undefined
                });
              } else {
                console.warn(`API call for ${screener.id} was successful but data was not valid:`, result); // ADDED LOG
              }
            } else {
              console.error(`HTTP error for ${screener.id}: ${response.status} ${response.statusText}`); // ADDED LOG
            }
          } catch (err) {
            console.warn(`Failed to fetch ${screener.id}:`, err);
          }
        }
        
        setScreenersData(results);
        console.log('Screeners data after fetch:', results); // ADDED LOG
        initializeSliderFilters(results);
        setError(null);
      } catch (err) {
        console.error('Error fetching screeners:', err);
        setError('Failed to fetch screener data');
      } finally {
        setLoading(false);
      }
    };

    fetchAllScreeners();
  }, [screeners]);

  const initializeSliderFilters = (screenersData: ScreenerData[]) => {
    if (screenersData.length === 0) return;
    
    // 모든 스크리너 데이터를 합쳐서 공통 숫자 컬럼 찾기
    const allData = screenersData.flatMap(screener => screener.data);
    if (allData.length === 0) return;
    
    const numericColumns = Object.keys(allData[0] || {}).filter(key => {
      // Symbol/ticker 컬럼 제외
      if (key.toLowerCase().includes('symbol') || key.toLowerCase().includes('ticker')) return false;
      
      // 숫자 컬럼만 선택
      const values = allData.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val));
      return values.length > 0;
    });

    const filters: SliderFilter[] = numericColumns.map(key => {
      const values = allData.map(item => item[key]).filter(val => typeof val === 'number' && !isNaN(val)) as number[];
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

  const toggleScreenerExpansion = (screenerId: string) => {
    setExpandedScreeners(prev => {
      const newSet = new Set(prev);
      if (newSet.has(screenerId)) {
        newSet.delete(screenerId);
      } else {
        newSet.add(screenerId);
      }
      return newSet;
    });
  };

  const getFilteredData = (data: ScreenerResult[]) => {
    return data.filter(item => {
      // 슬라이더 필터링
      return sliderFilters.every(filter => {
        const value = item[filter.key];
        if (typeof value !== 'number' || isNaN(value)) return true;
        return value >= filter.value[0] && value <= filter.value[1];
      });
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
  };

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading all screeners...</div>
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

  const totalScreeners = screeners.length;
  const totalResults = screenersData.reduce((sum, screener) => sum + screener.data.length, 0);
  const screenersWithData = screenersData.filter(screener => screener.data.length > 0).length;

  const handleRowClick = (item: Record<string, unknown>) => {
    setSelectedSymbol(String(item.symbol || item.ticker));
  };

  return (
    <div className="container mx-auto px-4 py-8 max-h-screen overflow-y-auto">
      <div className="mb-6">
        <Link 
          href="/" 
          className="text-blue-500 hover:text-blue-700 mb-4 inline-block"
        >
          ← Back to Dashboard
        </Link>
        
        {/* Screening Summary */}
        <div className="mb-8">
          <ScreeningSummary />
        </div>
        
        {/* TradingView Chart */}
        {selectedSymbol && (
          <div className="mt-6">
            <div className="bg-white rounded-lg shadow p-4">
              <h2 className="text-xl font-semibold text-gray-800 mb-4">
                {selectedSymbol} 차트
              </h2>
              <TradingViewChart symbol={selectedSymbol} height="500px" />
            </div>
          </div>
        )}
        
        {/* 요약 정보 */}
        {/* Quick Stats */}
        <div className="bg-gradient-to-r from-slate-50 to-gray-50 rounded-xl p-6 mb-6">
          <h2 className="text-lg font-semibold text-gray-800 mb-4 flex items-center space-x-2">
            <span>📊</span>
            <span>실시간 스크리닝 현황</span>
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-100">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-blue-100 rounded-lg flex items-center justify-center">
                  <span className="text-blue-600 text-lg">🔍</span>
                </div>
                <div>
                  <div className="text-2xl font-bold text-blue-600">{totalScreeners}</div>
                  <div className="text-sm text-gray-600">활성 스크리너</div>
                </div>
              </div>
            </div>
            <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-100">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-green-100 rounded-lg flex items-center justify-center">
                  <span className="text-green-600 text-lg">📈</span>
                </div>
                <div>
                  <div className="text-2xl font-bold text-green-600">{totalResults}</div>
                  <div className="text-sm text-gray-600">선별된 종목</div>
                </div>
              </div>
            </div>
            <div className="bg-white rounded-lg p-4 shadow-sm border border-gray-100">
              <div className="flex items-center space-x-3">
                <div className="w-10 h-10 bg-purple-100 rounded-lg flex items-center justify-center">
                  <span className="text-purple-600 text-lg">⚡</span>
                </div>
                <div>
                  <div className="text-2xl font-bold text-purple-600">{screenersWithData}</div>
                  <div className="text-sm text-gray-600">데이터 보유</div>
                </div>
              </div>
            </div>
          </div>
        </div>
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
              Global Filter Controls
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
      
      {/* 스크리너 결과 */}
      <div className="space-y-6">
        {screenersData.map((screenerData) => {
          const filteredData = getFilteredData(screenerData.data);
          const isExpanded = expandedScreeners.has(screenerData.type);
          const screener = screeners.find(s => s.id === screenerData.type);
          
          return (
            <div key={screenerData.type} className="bg-white rounded-lg shadow-lg overflow-hidden">
              <div 
                className="bg-gradient-to-r from-purple-600 to-blue-600 text-white p-4 cursor-pointer hover:from-purple-700 hover:to-blue-700 transition-all duration-300"
                onClick={() => toggleScreenerExpansion(screenerData.type)}
              >
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <span className="text-2xl">{screener?.icon}</span>
                    <div>
                      <h3 className="text-lg font-semibold">{screenerData.name}</h3>
                      <p className="text-purple-100 text-sm">
                        {filteredData.length} of {screenerData.data.length} results
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-4">
                    <Link
                      href={`/markminervini/${screenerData.type}`}
                      className="px-3 py-1 bg-white bg-opacity-20 rounded-md hover:bg-opacity-30 transition-colors text-sm"
                      onClick={(e) => e.stopPropagation()}
                    >
                      View Details
                    </Link>
                    <span className={`transform transition-transform duration-300 ${
                      isExpanded ? 'rotate-180' : ''
                    }`}>
                      ▼
                    </span>
                  </div>
                </div>
              </div>
              
              <div className={`overflow-hidden transition-all duration-500 ease-in-out ${
                isExpanded 
                  ? 'max-h-96 opacity-100' 
                  : 'max-h-0 opacity-0'
              }`}>
                {(() => { // Use an IIFE to log inside JSX
                  const filteredData = getFilteredData(screenerData.data);
                  console.log(`Filtered data for ${screenerData.name}:`, filteredData); // ADDED LOG
                  
                  // 간단한 컬럼 구성: 종목명과 시그널 발생일만 표시
                  const simpleColumns = [
                    {
                      key: 'symbol',
                      header: '종목명',
                      render: (item: Record<string, unknown>) => (
                        <span className="font-semibold text-purple-600">{String(item.symbol ?? 'N/A')}</span>
                      )
                    },
                    {
                      key: 'signal_date',
                      header: '시그널 발생일',
                      render: (item: Record<string, unknown>) => {
                        const value = item.signal_date;
                        // 날짜 형식 처리
                        if (value) {
                          try {
                            const date = new Date(value as string);
                            return date.toLocaleDateString('ko-KR');
                          } catch {
                            return String(value);
                          }
                        }
                        return 'N/A';
                      }
                    }
                  ];
                  return filteredData.length > 0 ? (
                    <>
                      {screenerData.lastUpdated && (
                        <div className="text-right text-xs text-gray-400 pr-4 pt-2">
                          Last updated: {new Date(screenerData.lastUpdated).toLocaleString()}
                        </div>
                      )}
                      <SimpleDataTable 
                        data={filteredData.slice(0, 10)} 
                        columns={simpleColumns}
                        description={`${filteredData.length}개 종목 중 최근 10개 표시${selectedSymbol ? '' : ' (종목을 클릭하면 차트를 볼 수 있습니다)'}`}
                        onRowClick={handleRowClick}
                      />
                      {filteredData.length > 10 && (
                        <div className="p-4 text-center text-gray-500 text-sm">
                          Showing 10 of {filteredData.length} results.
                          <Link
                            href={`/markminervini/${screenerData.type}`}
                            className="text-purple-600 hover:text-purple-800 ml-1"
                          >
                            View all →
                          </Link>
                        </div>
                      )}
                    </>
                  ) : (
                    <div className="p-8 text-center text-gray-500">
                      No data matches your current filters.
                    </div>
                  );
                })()}
              </div>
            </div>
          );
        })}
      </div>
      
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
