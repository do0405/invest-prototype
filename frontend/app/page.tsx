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
  const [summary, setSummary] = useState(null as SummaryData | null);
  const [dashboardSummary, setDashboardSummary] = useState(null as DashboardSummary | null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null as string | null);

  useEffect(() => {
    const fetchData = async () => {
      try {
        setLoading(true);
        const summaryResponse = await apiClient.getSummary();
        if (summaryResponse.success && summaryResponse.data) {
          setSummary(summaryResponse.data);
        }
        
        const dashboardResponse = await fetch('/api/dashboard-summary');
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
      <div className="flex justify-center items-center min-h-screen bg-background">
        <div className="text-lg text-muted-foreground">Loading...</div>
      </div>
    );
  }
  
  if (error) {
    return (
      <div className="flex justify-center items-center min-h-screen bg-background">
        <div className="text-destructive">
          <h2 className="text-xl font-semibold mb-2">Error</h2>
          <p>{error}</p>
          <button 
            onClick={() => window.location.reload()} 
            className="mt-4 px-4 py-2 bg-accent text-white rounded-notion hover:bg-accent-hover transition-colors"
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="container mx-auto px-4 py-8 bg-background min-h-screen">
      <h1 className="text-3xl font-semibold text-foreground mb-8">Investment Dashboard</h1>
      
      {/* 핵심 기능 섹션 */}
      <div className="mb-8">
        <Link href="/recent-signals" className="block">
          <div className="bg-card border border-border p-6 rounded-notion shadow-notion hover:shadow-notion-hover transition-shadow cursor-pointer">
            <h2 className="text-2xl font-semibold mb-2 text-foreground">🔥 최근 시그널</h2>
            {dashboardSummary?.data?.recent_signals && dashboardSummary.data.recent_signals.total_signals > 0 ? (
              <div>
                <p className="text-lg text-foreground">총 {dashboardSummary.data.recent_signals.total_signals}개 신규 시그널</p>
                <p className="text-sm text-muted-foreground">{dashboardSummary.data.recent_signals.screeners_active}개 스크리너 활성</p>
                <p className="text-xs text-muted-foreground mt-2">
                  마지막 업데이트: {new Date(dashboardSummary.data.recent_signals.last_updated).toLocaleString('ko-KR')}
                </p>
              </div>
            ) : (
              <div>
                <p className="text-lg text-foreground">데이터 로딩 중...</p>
                <p className="text-sm text-muted-foreground">스크리너 결과를 확인하고 있습니다</p>
              </div>
            )}
            <p className="text-sm mt-3 text-muted-foreground">→ 클릭하여 상세 보기</p>
          </div>
        </Link>
      </div>
      
      {/* 시장 국면 모니터링 섹션 제거됨 */}
      
      {/* 시스템 개선 사항 알림 */}
      <div className="mb-8">
        <div className="bg-card border border-border p-6 rounded-notion shadow-notion">
          <h2 className="text-xl font-semibold mb-4 text-foreground">🚀 최신 시스템 개선 사항</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div className="bg-muted p-4 rounded-notion">
              <h3 className="text-lg font-semibold text-success mb-2">⚡ RS 점수 계산 최적화</h3>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• 메모리 경합 문제 해결</li>
                <li>• 청크 단위 처리로 안정성 향상</li>
                <li>• 가비지 컬렉션 자동화</li>
                <li>• 스레드 안전성 보장</li>
              </ul>
            </div>
            <div className="bg-muted p-4 rounded-notion">
              <h3 className="text-lg font-semibold text-accent mb-2">🔧 병렬 처리 안정성 강화</h3>
              <ul className="text-sm text-muted-foreground space-y-1">
                <li>• 파일 I/O 경합 방지</li>
                <li>• API 레이트 리미트 관리</li>
                <li>• 프로세스 중복 실행 방지</li>
                <li>• 메모리 사용량 모니터링</li>
              </ul>
            </div>
          </div>
          <div className="mt-4 p-3 bg-success/10 border border-success/20 rounded-notion">
            <p className="text-sm text-success">
              <strong>📈 성능 향상:</strong> RS 점수 계산 속도 개선 및 시스템 안정성 대폭 향상으로 더욱 신뢰할 수 있는 스크리닝 결과를 제공합니다.
            </p>
          </div>
        </div>
      </div>
      
      {/* 새로운 스크리닝 섹션 - 대시보드 데이터 사용 */}
      {dashboardSummary?.data?.screeners_status && (
        <div>
          <h2 className="text-2xl font-semibold text-foreground mb-6">스크리닝 결과</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {/* 기술적 스크리닝 */}
            <Link href="/screening/technical" className="block">
              <div className="bg-card border border-border p-6 rounded-notion shadow-notion hover:shadow-notion-hover transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-accent">기술적 스크리닝</h2>
                {dashboardSummary.data.screeners_status.technical?.available ? (
                  <div>
                    <p className="text-foreground">총 {dashboardSummary.data.screeners_status.technical.count}개 종목</p>
                    <p className="text-sm text-muted-foreground mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.technical.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-muted-foreground">데이터 준비 중...</p>
                )}
                <p className="text-sm text-muted-foreground mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
            
            {/* 재무제표 스크리닝 */}
            <Link href="/screening/financial" className="block">
              <div className="bg-card border border-border p-6 rounded-notion shadow-notion hover:shadow-notion-hover transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-success">재무제표 스크리닝</h2>
                {dashboardSummary.data.screeners_status.financial?.available ? (
                  <div>
                    <p className="text-foreground">총 {dashboardSummary.data.screeners_status.financial.count}개 종목</p>
                    <p className="text-sm text-muted-foreground mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.financial.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-muted-foreground">데이터 준비 중...</p>
                )}
                <p className="text-sm text-muted-foreground mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
            
            {/* 통합 스크리닝 */}
            <Link href="/screening/integrated" className="block">
              <div className="bg-card border border-border p-6 rounded-notion shadow-notion hover:shadow-notion-hover transition-shadow cursor-pointer">
                <h2 className="text-xl font-semibold mb-2 text-accent">통합 스크리닝</h2>
                {dashboardSummary.data.screeners_status.integrated?.available ? (
                  <div>
                    <p className="text-foreground">총 {dashboardSummary.data.screeners_status.integrated.count}개 종목</p>
                    <p className="text-sm text-muted-foreground mt-2">
                      업데이트: {new Date(dashboardSummary.data.screeners_status.integrated.last_updated).toLocaleDateString('ko-KR')}
                    </p>
                  </div>
                ) : (
                  <p className="text-muted-foreground">데이터 준비 중...</p>
                )}
                <p className="text-sm text-muted-foreground mt-2">클릭하여 상세 보기</p>
              </div>
            </Link>
          </div>
        </div>
      )}
      
      {/* 기존 전략 섹션 유지 */}
      {summary && Object.keys(summary.strategies).length > 0 && (
        <div className="mt-8">
          <h2 className="text-2xl font-semibold text-foreground mb-6">전략별 현황</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {Object.entries(summary.strategies).map(([strategy, data]) => {
              const strategyData = data as { count: number; active_positions: number };
              return (
                <Link key={strategy} href={`/strategy/${strategy}`} className="block">
                  <div className="bg-card border border-border p-6 rounded-notion shadow-notion hover:shadow-notion-hover transition-shadow cursor-pointer">
                    <h2 className="text-xl font-semibold mb-2 text-warning">{strategy}</h2>
                    <p className="text-foreground">포지션: {strategyData.active_positions}개</p>
                    <p className="text-foreground">총 종목: {strategyData.count}개</p>
                    <p className="text-sm text-muted-foreground mt-2">클릭하여 포트폴리오 보기</p>
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      )}
      
      {!dashboardSummary && !summary && !loading && !error && (
        <div className="text-center text-muted-foreground">
          <p>데이터를 불러오는 중입니다...</p>
        </div>
      )}
    </div>
  );
}
