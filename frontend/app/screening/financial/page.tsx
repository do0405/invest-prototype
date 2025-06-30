'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import SimpleDataTable from '@/components/SimpleDataTable';
import TradingViewChart from '@/components/TradingViewChart';
import { apiClient, ScreeningData } from '@/lib/api';

export default function FinancialScreeningPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [selectedSymbol, setSelectedSymbol] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getFinancialResults();
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
        <span className="font-semibold text-green-600">{String(item.symbol ?? 'N/A')}</span>
      )
    },
    {
      key: 'signal_date',
      header: '재무 시그널 발생일',
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
        <div className="text-lg">Loading financial screening results...</div>
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
      <h1 className="text-3xl font-bold text-gray-800 mb-6">Financial Screening Results</h1>
      
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
      
      <div className="mb-6 p-4 bg-green-50 rounded-lg">
        <h2 className="text-lg font-semibold text-green-800 mb-2">재무제표 분석 스크리닝</h2>
        <p className="text-green-700">
          기업의 재무제표를 분석하여 건전한 재무구조와 성장 잠재력을 가진 종목들을 선별합니다.
          매출 성장률, 영업이익률, ROE, 부채비율 등의 재무지표를 종합적으로 평가합니다.
          {selectedSymbol ? '' : ' 종목을 클릭하면 차트를 볼 수 있습니다.'}
        </p>
      </div>
      {data.length > 0 ? (
        <SimpleDataTable 
          data={data} 
          columns={simpleColumns}
          title="재무제표 분석 우량 종목"
          description={`총 ${data.length}개 종목에서 우수한 재무지표 확인`}
          onRowClick={handleRowClick}
        />
      ) : (
        <div className="text-center text-gray-500">No data available</div>
      )}
    </div>
  );
}