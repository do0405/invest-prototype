'use client';
import { useEffect, useState, useMemo } from 'react';
import Link from 'next/link';
import SimpleDataTable from '@/components/SimpleDataTable';
import EnhancedDataTable from '@/components/EnhancedDataTable';
import TradingViewChart from '@/components/TradingViewChart';
import ScreeningSummary from '@/components/ScreeningSummary';
import AlgorithmDescription from '@/components/AlgorithmDescription';
// NumberInputFilter 제거됨 - 슬라이더 기반 필터 제거
import { MagnifyingGlassIcon, ChartBarIcon, CalendarIcon, TrophyIcon } from '@heroicons/react/24/outline';

interface ScreenerResult {
  symbol: string;
  rs_score?: number;
  signal_date?: string;
  met_count?: number;
  // Pattern detection fields (both naming conventions)
  vcp_detected?: boolean;
  VCP_Pattern?: boolean;
  cup_handle_detected?: boolean;
  Cup_Handle_Pattern?: boolean;
  vcp_confidence?: number;
  cup_handle_confidence?: number;
  [key: string]: string | number | boolean | null | undefined;
}

interface ScreenerData {
  name: string;
  data: ScreenerResult[];
  type: string;
  lastUpdated?: string;
}

// SliderFilter 인터페이스는 NumberInputFilter 컴포넌트의 NumberFilter로 대체됨

type SortOption = 'symbol' | 'rs_score' | 'signal_date' | 'met_count';
type SortDirection = 'asc' | 'desc';

export default function AllMarkminerviniPage() {
  const [screenersData, setScreenersData] = useState<ScreenerData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);
  const [expandedScreeners, setExpandedScreeners] = useState<Set<string>>(new Set());
  // 슬라이더 기반 필터 제거됨
  const [searchTerm, setSearchTerm] = useState('');
  const [sortBy, setSortBy] = useState<SortOption>('rs_score');
  const [sortDirection, setSortDirection] = useState<SortDirection>('desc');
  const [showChart, setShowChart] = useState(true);
  const [activeTab, setActiveTab] = useState<'all' | 'new_tickers'>('all');

  // 패턴이 적용된 3가지 결과만 표시 (고도화 단계별)
  const screeners = useMemo(() => [
    { id: 'image_pattern_results', name: '1단계: 이미지 패턴 분석', icon: '🖼️', description: 'VCP, Cup & Handle 패턴 이미지 분석 결과' },
    { id: 'integrated_pattern_results', name: '2단계: 통합 패턴 분석', icon: '🔗', description: '수학적 알고리즘 기반 패턴 검증 결과' },
    { id: 'integrated_results', name: '3단계: 패턴 인식 전 결과', icon: '🎯', description: '기술적+재무적 조건을 만족하는 패턴 인식 전 결과' },
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
        // 슬라이더 기반 필터 초기화 제거됨
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

  // 슬라이더 기반 필터 초기화 제거됨

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

  const getFilteredAndSortedData = (data: ScreenerResult[], screenerType?: string) => {
    let filteredData = data.filter(item => {
      // 검색어 필터링
      const searchMatch = !searchTerm || 
        item.symbol?.toString().toLowerCase().includes(searchTerm.toLowerCase());
      
      // 스크리너별 특별 필터링
      if (screenerType === 'image_pattern_results') {
        // 이미지 패턴 분석: VCP 또는 Cup&Handle이 감지된 것만
        const hasVcpPattern = item.vcp_detected === true || item.VCP_Pattern === true;
        const hasCupHandlePattern = item.cup_handle_detected === true || item.Cup_Handle_Pattern === true;
        return searchMatch && (hasVcpPattern || hasCupHandlePattern);
      } else if (screenerType === 'integrated_pattern_results') {
        // 통합 패턴 분석: confidence level이 High인 것만
        const hasHighVcpConfidence = item.vcp_confidence_level === 'High';
        const hasHighCupHandleConfidence = item.cup_handle_confidence_level === 'High';
        return searchMatch && (hasHighVcpConfidence || hasHighCupHandleConfidence);
      }
      
      return searchMatch;
    });

    // 정렬
    filteredData.sort((a, b) => {
      const aValue = a[sortBy];
      const bValue = b[sortBy];
      
      if (aValue === null || aValue === undefined) return 1;
      if (bValue === null || bValue === undefined) return -1;
      
      let comparison = 0;
      if (typeof aValue === 'string' && typeof bValue === 'string') {
        comparison = aValue.localeCompare(bValue);
      } else if (typeof aValue === 'number' && typeof bValue === 'number') {
        comparison = aValue - bValue;
      } else {
        comparison = String(aValue).localeCompare(String(bValue));
      }
      
      return sortDirection === 'asc' ? comparison : -comparison;
    });

    return filteredData;
  };



  const resetFilters = () => {
    setSearchTerm('');
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

  const handleRowClick = (symbol: string) => {
    setSelectedSymbol(symbol);
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
        
         <h1 className="text-4xl font-bold text-gray-900 mb-4 flex items-center gap-3">
          <span className="text-5xl">🎯</span>
          Mark Minervini 패턴 분석 결과
        </h1>
        <p className="text-xl text-gray-600 mb-8">
          Mark Minervini의 투자 전략을 기반으로 한 고도화된 패턴 분석 결과입니다. 기술적 조건 → 재무 조건 → 패턴 적용 순서로 진행된 3단계 스크리닝 결과를 확인하실 수 있습니다.
        </p>
        
        {/* Algorithm Description */}
        <div className="mb-8">
          <AlgorithmDescription algorithm="markminervini_comprehensive" />
        </div>
        
        {/* Screening Summary */}
        <div className="mb-8">
          <ScreeningSummary />
        </div>
        
        {/* TradingView Chart */}
        {selectedSymbol && showChart && (
          <div className="mt-6">
            <div className="bg-white rounded-lg shadow p-4">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-xl font-semibold text-gray-800 flex items-center gap-2">
                  <ChartBarIcon className="h-6 w-6 text-purple-600" />
                  {selectedSymbol} 차트
                </h2>
                <button
                  onClick={() => setSelectedSymbol(null)}
                  className="text-gray-400 hover:text-gray-600 transition-colors"
                >
                  ✕
                </button>
              </div>
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

      {/* 마크 미니버니 스크리너 기준 설명 */}
      <div className="mb-6 bg-gradient-to-r from-blue-50 to-purple-50 rounded-lg border border-blue-200 p-6">
        <div className="flex items-start gap-4">
          <div className="flex-shrink-0">
            <div className="w-12 h-12 bg-blue-100 rounded-full flex items-center justify-center">
              <span className="text-2xl">📊</span>
            </div>
          </div>
          <div className="flex-1">
            <h3 className="text-lg font-semibold text-gray-800 mb-2">
              마크 미니버니 트렌드 템플릿 기준
            </h3>
            <p className="text-sm text-gray-600 mb-4">
              성장주 투자의 대가 마크 미니버니가 개발한 8가지 기술적 분석 조건을 기반으로 한 스크리닝 시스템입니다.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-green-100 text-green-600 rounded-full flex items-center justify-center text-xs font-bold">1</span>
                  <span>현재가 &gt; 150일 및 200일 이동평균</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-green-100 text-green-600 rounded-full flex items-center justify-center text-xs font-bold">2</span>
                  <span>150일 이동평균 &gt; 200일 이동평균</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-green-100 text-green-600 rounded-full flex items-center justify-center text-xs font-bold">3</span>
                  <span>200일 이동평균이 최소 1개월간 상승</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-green-100 text-green-600 rounded-full flex items-center justify-center text-xs font-bold">4</span>
                  <span>50일 이동평균 &gt; 150일 및 200일 이동평균</span>
                </div>
              </div>
              <div className="space-y-2">
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-bold">5</span>
                  <span>현재가가 52주 최저가보다 30% 이상 높음</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-bold">6</span>
                  <span>현재가가 52주 최고가의 75% 이상</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center text-xs font-bold">7</span>
                  <span>현재가 &gt; 20일 이동평균</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className="w-6 h-6 bg-purple-100 text-purple-600 rounded-full flex items-center justify-center text-xs font-bold">8</span>
                  <span>RS Rating ≥ 85 (상대 강도 점수)</span>
                </div>
              </div>
            </div>
            <div className="mt-4 p-3 bg-yellow-50 border border-yellow-200 rounded-lg">
              <p className="text-xs text-yellow-800">
                <strong>💡 참고:</strong> 모든 8가지 조건을 만족하는 종목만이 최종 선별되며, RS Rating 기준으로 정렬됩니다.
              </p>
            </div>
          </div>
        </div>
      </div>

      {/* 탭 네비게이션 */}
      <div className="mb-6">
        <div className="flex space-x-1 bg-gray-100 p-1 rounded-lg">
          <button
            onClick={() => setActiveTab('all')}
            className={`px-4 py-2 rounded-md transition-all duration-200 ${
              activeTab === 'all'
                ? 'bg-white text-purple-600 shadow-sm font-medium'
                : 'text-gray-600 hover:text-gray-800'
            }`}
          >
            전체 스크리너
          </button>
          <button
            onClick={() => setActiveTab('new_tickers')}
            className={`px-4 py-2 rounded-md transition-all duration-200 flex items-center gap-2 ${
              activeTab === 'new_tickers'
                ? 'bg-white text-purple-600 shadow-sm font-medium'
                : 'text-gray-600 hover:text-gray-800'
            }`}
          >
            <span>🆕</span>
            신규 티커
          </button>
        </div>
      </div>

      {/* 검색 및 정렬 컨트롤 */}
      <div className="mb-6 bg-white rounded-lg shadow-sm border border-gray-200 p-4">
        <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
          {/* 검색 */}
          <div className="relative">
            <MagnifyingGlassIcon className="absolute left-3 top-1/2 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
            <input
              type="text"
              placeholder="티커 검색..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
            />
          </div>

          {/* 정렬 기준 */}
          <div>
            <select
              value={sortBy}
              onChange={(e) => setSortBy(e.target.value as SortOption)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
            >
              <option value="rs_score">RS 점수</option>
              <option value="symbol">티커</option>
              <option value="signal_date">시그널 날짜</option>
              <option value="met_count">충족 조건 수</option>
            </select>
          </div>

          {/* 정렬 방향 */}
          <div>
            <select
              value={sortDirection}
              onChange={(e) => setSortDirection(e.target.value as SortDirection)}
              className="w-full px-3 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-purple-500 focus:border-transparent"
            >
              <option value="desc">높은 순</option>
              <option value="asc">낮은 순</option>
            </select>
          </div>

          {/* 차트 표시 토글 */}
          <div className="flex items-center">
            <label className="flex items-center cursor-pointer">
              <input
                type="checkbox"
                checked={showChart}
                onChange={(e) => setShowChart(e.target.checked)}
                className="sr-only"
              />
              <div className={`relative w-11 h-6 rounded-full transition-colors duration-200 ${
                showChart ? 'bg-purple-600' : 'bg-gray-300'
              }`}>
                <div className={`absolute top-0.5 left-0.5 w-5 h-5 bg-white rounded-full transition-transform duration-200 ${
                  showChart ? 'translate-x-5' : 'translate-x-0'
                }`} />
              </div>
              <span className="ml-3 text-sm text-gray-700 flex items-center gap-1">
                <ChartBarIcon className="h-4 w-4" />
                차트 표시
              </span>
            </label>
          </div>
        </div>
      </div>

      {/* 슬라이더 기반 필터 제거됨 */}
      
      {/* 스크리너 결과 */}
      {activeTab === 'all' && (
        <div className="space-y-6">
          {screenersData.map((screenerData) => {
            const filteredData = getFilteredAndSortedData(screenerData.data, screenerData.type);
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
                        <p className="text-purple-100 text-xs mb-1">{screener?.description}</p>
                        <p className="text-purple-100 text-sm flex items-center gap-4">
                          <span className="flex items-center gap-1">
                            <TrophyIcon className="h-4 w-4" />
                            {filteredData.length} of {screenerData.data.length} results
                          </span>
                          {screenerData.data.length > 0 && screenerData.data[0].signal_date && (
                            <span className="flex items-center gap-1">
                              <CalendarIcon className="h-4 w-4" />
                              최신: {new Date(screenerData.data[0].signal_date as string).toLocaleDateString('ko-KR')}
                            </span>
                          )}
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
                    const filteredData = getFilteredAndSortedData(screenerData.data);
                    console.log(`Filtered data for ${screenerData.name}:`, filteredData); // ADDED LOG
                    
                    // 스크리너별 컬럼 구성
                    const availableColumns = filteredData.length > 0 ? Object.keys(filteredData[0]) : [];
                    const simpleColumns = [
                      {
                        key: 'symbol',
                        header: '종목명',
                        render: (item: Record<string, unknown>) => (
                          <span className="font-semibold text-purple-600">{String(item.symbol ?? 'N/A')}</span>
                        )
                      }
                    ];
                    
                    // 날짜 컬럼 추가 (우선순위: signal_date > detection_date > processing_date)
                    if (availableColumns.includes('signal_date') || availableColumns.includes('detection_date') || availableColumns.includes('processing_date')) {
                      simpleColumns.push({
                        key: 'date',
                        header: '날짜',
                        render: (item: Record<string, unknown>) => {
                          const value = item.signal_date || item.detection_date || item.processing_date;
                          if (value) {
                            try {
                              const date = new Date(value as string);
                              return <span>{date.toLocaleDateString('ko-KR')}</span>;
                            } catch {
                              return <span>{String(value)}</span>;
                            }
                          }
                          return <span>N/A</span>;
                         }
                       });
                     }
                     
                     // RS 점수 컬럼 추가
                     if (availableColumns.includes('rs_score')) {
                       simpleColumns.push({
                         key: 'rs_score',
                         header: 'RS 점수',
                         render: (item: Record<string, unknown>) => {
                           const value = item.rs_score;
                           return <span>{value ? Number(value).toFixed(2) : 'N/A'}</span>;
                         }
                       });
                     }
                     
                     // 스크리너별 특화 컬럼 추가
                     if (screenerData.type === 'integrated_pattern_results') {
                       // 통합 패턴 분석: confidence 값과 level만 표시
                       if (availableColumns.includes('vcp_confidence')) {
                         simpleColumns.push({
                           key: 'vcp_confidence',
                           header: 'VCP 신뢰도',
                           render: (item: Record<string, unknown>) => {
                             const confidence = item.vcp_confidence;
                             const level = item.vcp_confidence_level;
                             return <span>{confidence ? `${Number(confidence).toFixed(3)} (${level})` : 'N/A'}</span>;
                           }
                         });
                       }
                       if (availableColumns.includes('cup_handle_confidence')) {
                         simpleColumns.push({
                           key: 'cup_handle_confidence',
                           header: 'C&H 신뢰도',
                           render: (item: Record<string, unknown>) => {
                             const confidence = item.cup_handle_confidence;
                             const level = item.cup_handle_confidence_level;
                             return <span>{confidence ? `${Number(confidence).toFixed(3)} (${level})` : 'N/A'}</span>;
                           }
                         });
                       }
                     } else {
                       // 다른 스크리너: 기존 VCP/C&H 패턴 컬럼
                       if (availableColumns.includes('vcp_detected')) {
                         simpleColumns.push({
                           key: 'vcp_detected',
                           header: 'VCP',
                           render: (item: Record<string, unknown>) => {
                             const value = item.vcp_detected;
                             return <span>{value === true ? '✓' : value === false ? '✗' : 'N/A'}</span>;
                           }
                         });
                       }
                       
                       if (availableColumns.includes('cup_handle_detected')) {
                         simpleColumns.push({
                           key: 'cup_handle_detected',
                           header: 'C&H',
                           render: (item: Record<string, unknown>) => {
                             const value = item.cup_handle_detected;
                             return <span>{value === true ? '✓' : value === false ? '✗' : 'N/A'}</span>;
                           }
                         });
                       }
                     }
                    return filteredData.length > 0 ? (
                      <>
                        {screenerData.lastUpdated && (
                          <div className="text-right text-xs text-gray-400 pr-4 pt-2">
                            Last updated: {new Date(screenerData.lastUpdated).toLocaleString()}
                          </div>
                        )}
                        <EnhancedDataTable 
                           data={filteredData.slice(0, 10)} 
                           onRowClick={handleRowClick}
                           showChart={showChart}
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
      )}

      {/* 신규 티커 탭 */}
      {activeTab === 'new_tickers' && (
        <div className="bg-white rounded-lg shadow border border-gray-200">
          <div className="p-6">
            <div className="text-center py-12">
              <span className="text-6xl mb-4 block">🆕</span>
              <h3 className="text-xl font-semibold text-gray-800 mb-2">
                신규 티커 전용 탭
              </h3>
              <p className="text-gray-600 mb-6">
                최근 새롭게 발견된 티커들을 별도로 관리하고 추적할 수 있습니다.
              </p>
              <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 max-w-md mx-auto">
                <p className="text-sm text-blue-800">
                  💡 이 기능은 향후 업데이트에서 구현될 예정입니다.
                </p>
              </div>
            </div>
          </div>
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
