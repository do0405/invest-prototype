'use client';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import DataTable, { DataTableColumn } from '@/components/DataTable';
import AlgorithmDescription from '@/components/AlgorithmDescription';
import { apiClient, ScreeningData } from '@/lib/api';

export default function PatternDetectionResultsPage() {
  const [data, setData] = useState<ScreeningData[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchData = async () => {
      const res = await apiClient.getMarkminerviniResults('pattern_detection_results');
      if (res.success && res.data) {
        // cup&handle 또는 VCP 패턴이 감지되고 confidence가 임계값 이상인 항목만 필터링
        const filteredData = res.data.filter((item: any) => {
          // 패턴 감지 여부 확인 (두 가지 필드명 모두 지원)
          const vcpDetected = item.vcp_detected === true || item.VCP_Pattern === true;
          const cupHandleDetected = item.cup_handle_detected === true || item.Cup_Handle_Pattern === true;
          
          // confidence 값 검증 (숫자이고 유효한 값인지 확인)
          const vcpConfidence = typeof item.vcp_confidence === 'number' ? item.vcp_confidence : 0;
          const cupHandleConfidence = typeof item.cup_handle_confidence === 'number' ? item.cup_handle_confidence : 0;
          
          // main.py의 enhanced_pattern_analyzer.py에서 사용하는 DETECTION_THRESHOLD = 0.6 적용
          const vcpConfidenceValid = vcpConfidence >= 0.6;
          const cupHandleConfidenceValid = cupHandleConfidence >= 0.6;
          
          // 패턴이 감지되고 해당 confidence가 임계값 이상인 경우만 포함
          const vcpValid = vcpDetected && vcpConfidenceValid;
          const cupHandleValid = cupHandleDetected && cupHandleConfidenceValid;
          
          return vcpValid || cupHandleValid;
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

  const columns: DataTableColumn<ScreeningData>[] = data.length
    ? Object.keys(data[0]).slice(0, 8).map(key => ({
        key,
        header: key.replace(/_/g, ' ').replace(/\b\w/g, l => l.toUpperCase()),
      }))
    : [];

  if (loading) {
    return (
      <div className="flex justify-center items-center min-h-screen">
        <div className="text-lg">Loading pattern detection results...</div>
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
          📊 Mark Minervini Pattern Detection Results
        </h1>
        <p className="text-gray-600">
          패턴 인식을 통한 주식 스크리닝 결과입니다.
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
      
      {/* Algorithm Description */}
      <div className="mb-8">
        <AlgorithmDescription algorithm="markminervini_pattern_detection" />
      </div>
      
      <DataTable 
        data={data} 
        columns={columns}
        title="Pattern Detection Results"
        description={`총 ${data.length}개의 패턴 감지 결과`}
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