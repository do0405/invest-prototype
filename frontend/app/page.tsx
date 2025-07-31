'use client';

import { useEffect, useState } from 'react';
import { apiClient, SummaryData } from '@/lib/api';
import Link from 'next/link';

interface DashboardSummary {
  success: boolean;
  data: {
    recent_signals: {
      total_signals: number;
      screeners_active: number;
      last_updated: string;
    };
    top_stocks: {
      available: boolean;
      top_score: number;
      last_updated: string;
    };
    market_regime: {
      current_regime: string;
      confidence: number;
      last_updated: string;
    };
    screeners_status: {
      [key: string]: {
        available: boolean;
        count: number;
        last_updated: string;
      };
    };
  };
  message?: string;
}

export default function HomePage() {
  const [summary, setSummary] = useState<SummaryData | null>(null);
  const [dashboardSummary, setDashboardSummary] = useState<DashboardSummary | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        
        // 기존 요약 데이터 가져오기
        const summaryResponse = await apiClient.getSummary();
        if (summaryResponse.success && summaryResponse.data) {
          setSummary(summaryResponse.data);
        }
        
        // 새로운 대시보드 요약 데이터 가져오기
        const dashboardResponse = await fetch('http://localhost:5000/api/dashboard-summary');
        const dashboardResult: DashboardSummary = await dashboardResponse.json();
        
        if (dashboardResult.success && dashboardResult.data) {
          setDashboardSummary(dashboardResult);
        }
        
        setError(null);
      } catch (err) {
        console.error('Error fetching data:', err);
        setError('Network error occurred');
      } finally {
        setLoading(false);
      }
    };

    fetchData();
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
      
      {/* 핵심 기능 섹션 */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        {/* 최근 시그널 */}
        <Link href="/recent-signals" className="block">
          <div className="bg-gradient-to-r from-blue-500 to-blue-600 text-white p-6 rounded-lg shadow-lg hover:shadow-xl transition-shadow cursor-pointer">
            <h2 className="text-2xl font-bold mb-2">🔥 최근 시그널</h2>
            {dashboardSummary?.data?.recent_signals && dashboardSummary.data.recent_signals.total_signals > 0 ? (
              <div>
                <p className="text-lg">총 {dashboardSummary.data.recent_signals.total_signals}개 신규 시그널</p>
                <p className="text-sm opacity-90">{dashboardSummary.data.recent_signals.screeners_active}개 스크리너 활성</p>
                <p className="text-xs opacity-75 mt-2">
                  마지막 업데이트: {new Date(dashboardSummary.data.recent_signals.last_updated).toLocaleString('ko-KR')}
                </p>
              </div>
            ) : (
              <div>
                <p className="text-lg">데이터 로딩 중...</p>
                <p className="text-sm opacity-90">스크리너 결과를 확인하고 있습니다</p>
              </div>
            )}
            <p className="text-sm mt-3 opacity-90">→ 클릭하여 상세 보기</p>
          </div>
        </Link>
        
        {/* Top 10 매수 랭킹 */}
        <Link href="/top-recommendations" className="block">
          <div className="bg-gradient-to-r from-green-500 to-green-600 text-white p-6 rounded-lg shadow-lg hover:shadow-xl transition-shadow cursor-pointer">
            <h2 className="text-2xl font-bold mb-2">⭐ Top 10 매수 랭킹</h2>
            {dashboardSummary?.data?.top_stocks?.available ? (
              <div>
                <p className="text-lg">TOPSIS 기반 종합 평가</p>
                <p className="text-sm opacity-90">최고 점수: {dashboardSummary.data.top_stocks.top_score.toFixed(4)}</p>
                <p className="text-xs opacity-75 mt-2">
                  마지막 업데이트: {new Date(dashboardSummary.data.top_stocks.last_updated).toLocaleString('ko-KR')}
                </p>
              </div>
            ) : (
              <div>
                <p className="text-lg">랭킹 데이터 준비 중...</p>
                <p className="text-sm opacity-90">TOPSIS 분석 결과를 로딩하고 있습니다</p>
              </div>
            )}
            <p className="text-sm mt-3 opacity-90">→ 클릭하여 상세 보기</p>
          </div>
        </Link>
      </div>
      
      {/* 시장 현황 */}
      {dashboardSummary?.data?.market_regime && (
        <div className="mb-8">
          <div className="bg-white p-6 rounded-lg shadow">
            <h2 className="text-xl font-semibold mb-4 text-gray-800">📊 시장 현황</h2>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="text-center">
                <div className="text-2xl font-bold text-purple-600">
                  {dashboardSummary.data.market_regime.current_regime === 'Unknown' ? '분석 중' : dashboardSummary.data.market_regime.current_regime}
                </div>
                <div className="text-sm text-gray-600">현재 시장 체제</div>
              </div>
              <div className="text-center">
                <div className="text-2xl font-bold text-blue-600">
                  {dashboardSummary.data.market_regime.confidence > 0 ? (dashboardSummary.data.market_regime.confidence * 100).toFixed(1) + '%' : '계산 중'}
                </div>
                <div className="text-sm text-gray-600">신뢰도</div>
              </div>
              <div className="text-center">
                <div className="text-sm text-gray-600">
                  업데이트: {new Date(dashboardSummary.data.market_regime.last_updated).toLocaleDateString('ko-KR')}
                </div>
              </div>
            </div>
          </div>
        </div>
      )}
      
      {/* 시스템 개선 사항 알림 */}
      <div className="mb-8">
        <div className="bg-gradient-to-r from-emerald-50 to-teal-50 border border-emerald-200 p-6 rounded-lg">
          <h2 className="text-xl font-semibold mb-4 text-emerald-800">🚀 최신 시스템 개선 사항</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-white p-4 rounded-lg shadow-sm">
              <h3 className="text-lg font-semibold text-emerald-700 mb-2">⚡ RS 점수 계산 최적화</h3>
              <ul className="text-sm text-gray-700 space-y-1">
                <li>• 메모리 경합 문제 해결</li>
                <li>• 청크 단위 처리로 안정성 향상</li>
                <li>• 가비지 컬렉션 자동화</li>
                <li>• 스레드 안전성 보장</li>
              </ul>
            </div>
            <div className="bg-white p-4 rounded-lg shadow-sm">
              <h3 className="text-lg font-semibold text-blue-700 mb-2">🔧 병렬 처리 안정성 강화</h3>
              <ul className="text-sm text-gray-700 space-y-1">
                <li>• 파일 I/O 경합 방지</li>
                <li>• API 레이트 리미트 관리</li>
                <li>• 프로세스 중복 실행 방지</li>
                <li>• 메모리 사용량 모니터링</li>
              </ul>
            </div>
          </div>
          <div className="mt-4 p-3 bg-emerald-100 rounded-lg">
            <p className="text-sm text-emerald-800">
              <strong>📈 성능 향상:</strong> RS 점수 계산 속도 개선 및 시스템 안정성 대폭 향상으로 더욱 신뢰할 수 있는 스크리닝 결과를 제공합니다.
            </p>
          </div>
        </div>
      </div>
      
      {/* 새로운 스크리닝 섹션 - 대시보드 데이터 사용 */}
      {dashboardSummary?.data?.screeners_status && (
        <div>
          <h2 className="text-2xl font-semibold text-gray-800 mb-6">스크리닝 결과</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {/* 기술적 스크리닝 */}
            <Link href="/screening/technical" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-blue-600">기술적 스크리닝</h2>
                {dashboardSummary.data.screeners_status.technical?.available ? (
                  <div>
                    <p className="text-gray-600">총 {dashboardSummary.data.screeners_status.technical.count}개 종목</p>
                    <p className="text-sm text-gray-500 mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.technical.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-gray-500">데이터 준비 중...</p>
                )}
                <p className="text-sm text-gray-500 mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
            
            {/* 재무제표 스크리닝 */}
            <Link href="/screening/financial" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-green-600">재무제표 스크리닝</h2>
                {dashboardSummary.data.screeners_status.financial?.available ? (
                  <div>
                    <p className="text-gray-600">총 {dashboardSummary.data.screeners_status.financial.count}개 종목</p>
                    <p className="text-sm text-gray-500 mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.financial.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-gray-500">데이터 준비 중...</p>
                )}
                <p className="text-sm text-gray-500 mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
            
            {/* 통합 스크리닝 */}
            <Link href="/screening/integrated" className="block">
              <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-purple-600">통합 스크리닝</h2>
                {dashboardSummary.data.screeners_status.integrated?.available ? (
                  <div>
                    <p className="text-gray-600">총 {dashboardSummary.data.screeners_status.integrated.count}개 종목</p>
                    <p className="text-sm text-gray-500 mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.integrated.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-gray-500">데이터 준비 중...</p>
                )}
                <p className="text-sm text-gray-500 mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
          </div>
        </div>
      )}
      
      {/* 기존 전략 섹션 유지 */}
      {summary && Object.keys(summary.strategies).length > 0 && (
        <div className="mt-8">
          <h2 className="text-2xl font-semibold text-gray-800 mb-6">전략별 현황</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {Object.entries(summary.strategies).map(([strategy, data]) => (
              <Link key={strategy} href={`/strategy/${strategy}`} className="block">
                <div className="bg-white p-6 rounded-lg shadow hover:shadow-lg transition-shadow cursor-pointer">
                  <h2 className="text-xl font-semibold mb-2 text-orange-600">{strategy}</h2>
                  <p className="text-gray-600">포지션: {data.active_positions}개</p>
                  <p className="text-gray-600">총 종목: {data.count}개</p>
                  <p className="text-sm text-gray-500 mt-2">클릭하여 포트폴리오 보기</p>
                </div>
              </Link>
            ))}
          </div>
        </div>
      )}
      
      {!dashboardSummary && !summary && !loading && !error && (
        <div className="text-center text-gray-500">
          <p>데이터를 불러오는 중입니다...</p>
        </div>
      )}
    </div>
  );
}