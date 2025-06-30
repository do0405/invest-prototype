'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import SimpleDataTable from '@/components/SimpleDataTable';
import TradingViewChart from '@/components/TradingViewChart';
import { apiClient, ScreeningData } from '@/lib/api';

export default function IntegratedScreeningPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getIntegratedResults();
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
      header: '통합 시그널 발생일',
      render: (item: Record<string, unknown>) => {
        const value = item.signal_date;
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

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading integrated screening results...</div>
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

  const handleRowClick = (item: Record<string, unknown>) => {
    setSelectedSymbol(item.symbol as string);
  };

  return (
    <div className="container mx-auto px-4 py-8">
      <Link href="/" className="text-blue-500 hover:text-blue-700 mb-4 inline-block">
        ← Back to Dashboard
      </Link>
      <h1 className="text-3xl font-bold text-gray-800 mb-6">Integrated Screening Results</h1>
      
      {/* TradingView Chart */}
      {selectedSymbol && (
        <div className="mb-6">
          <div className="bg-white rounded-lg shadow p-4">
            <h2 className="text-xl font-semibold text-gray-800 mb-4">
              {selectedSymbol} 차트
            </h2>
            <TradingViewChart symbol={selectedSymbol} height="500px" />
          </div>
        </div>
      )}
      
      <div className="mb-6 p-4 bg-purple-50 rounded-lg">
        <h2 className="text-lg font-semibold text-purple-800 mb-2">통합 분석 스크리닝</h2>
        <p className="text-purple-700">
          기술적 분석과 재무제표 분석을 종합하여 최종 투자 후보 종목을 선별합니다.
          두 가지 분석 방법에서 모두 긍정적인 신호를 보이는 종목들로, 보다 높은 투자 확신도를 제공합니다.
          기술적 매수 시그널과 펀더멘털 우수성을 동시에 만족하는 프리미엄 종목들입니다.
          {selectedSymbol ? '' : ' 종목을 클릭하면 차트를 볼 수 있습니다.'}
        </p>
      </div>
      {data.length > 0 ? (
        <SimpleDataTable 
          data={data} 
          columns={simpleColumns}
          title="통합 분석 최종 선별 종목"
          description={`총 ${data.length}개 종목이 기술적 분석과 재무 분석을 모두 통과`}
          onRowClick={handleRowClick}
        />
      ) : (
        <div className="text-center text-gray-500">No data available</div>
      )}
    </div>
  );
}