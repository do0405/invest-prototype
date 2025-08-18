'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import { apiClient, ScreeningData } from '@/lib/api';

export default function IntegratedPatternResultsPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getMarkminerviniResults('integrated_pattern_results');
      if (res.success && res.data) {
        // VCP 또는 Cup&Handle 패턴이 감지되고 'High' 신뢰도 레벨인 항목만 표시
        const filteredData = res.data.filter((item: any) => {
          const hasVcp = item.vcp_detected === true || item.VCP_Pattern === true;
          const hasCupHandle = item.cup_handle_detected === true || item.Cup_Handle_Pattern === true;
          const hasHighVcpConfidence = item.vcp_confidence_level === 'High';
          const hasHighCupHandleConfidence = item.cup_handle_confidence_level === 'High';
          
          return (hasVcp && hasHighVcpConfidence) || (hasCupHandle && hasHighCupHandleConfidence);
        });
        setData(filteredData);
        setError(null);
      } else {
        setError(res.message || 'Failed to fetch data');
      }
      setLoading(false);
    };
    fetchData();
  }, []);

  const columns: DataTableColumn<ScreeningData>[] = [
    {
      key: 'symbol',
      header: 'Symbol',
    },
    {
      key: 'processing_date',
      header: 'Processing Date',
    },
    {
      key: 'cup_handle_confidence_level',
      header: 'Cup Handle Confidence Level',
    },
    {
      key: 'vcp_confidence_level',
      header: 'Vcp Confidence Level',
    },
  ];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading integrated pattern results...</div>
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
          🔗 Mark Minervini 통합 패턴 분석 결과
        </h1>
        <p className="text-gray-600">
          수학적 알고리즘 기반 패턴 검증 결과입니다. VCP와 Cup & Handle 패턴을 다차원 평가 시스템으로 분석한 결과를 확인할 수 있습니다.
        </p>
        <div className="mt-4">
          <Link 
            href="/markminervini/all" 
            className="text-blue-600 hover:text-blue-800 underline"
          >
            ← 전체 Mark Minervini 결과로 돌아가기
          </Link>
        </div>
      </div>
      
      <DataTable 
        data={data} 
        columns={columns}
        title="Integrated Pattern Analysis Results"
        description={`총 ${data.length}개의 통합 패턴 분석 결과`}
        loading={loading}
        itemsPerPage={15}
        showPagination={true}
        paginationType="numbers"
        showInlineChart={true}
        chartHeight="500px"
      />
    </div>
  );
}