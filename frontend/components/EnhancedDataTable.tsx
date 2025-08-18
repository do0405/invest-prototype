'use client';

import React, { ReactNode } from 'react';
import { CalendarIcon, TrophyIcon, ChartBarIcon } from '@heroicons/react/24/outline';

interface EnhancedDataTableProps {
  data: Array<Record<string, unknown>>;
  onRowClick?: (symbol: string) => void;
  showChart?: boolean;
}

const EnhancedDataTable: React.FC<EnhancedDataTableProps> = ({ 
  data, 
  onRowClick,
  showChart = true 
}) => {
  if (!data || data.length === 0) {
    return (
      <div className="text-center py-8 text-gray-500">
        데이터가 없습니다.
      </div>
    );
  }

  const handleRowClick = (item: Record<string, unknown>) => {
    if (onRowClick && item.symbol) {
      onRowClick(String(item.symbol));
    }
  };

  // 데이터에서 사용 가능한 컬럼들을 동적으로 감지
  const availableColumns = data.length > 0 ? Object.keys(data[0]) : [];
  const hasRsScore = availableColumns.includes('rs_score');
  const hasMetCount = availableColumns.includes('met_count') || availableColumns.includes('fin_met_count');
  const hasSignalDate = availableColumns.includes('signal_date') || availableColumns.includes('detection_date') || availableColumns.includes('processing_date');
  const hasVcpDetected = availableColumns.includes('vcp_detected');
  const hasCupHandleDetected = availableColumns.includes('cup_handle_detected');
  const hasVcpConfidence = availableColumns.includes('vcp_confidence');
  const hasCupHandleConfidence = availableColumns.includes('cup_handle_confidence');
  
  // 다차원 평가 결과 컬럼 감지
  const hasVcpDimensional = availableColumns.some(col => col.includes('vcp_dimensional_scores'));
  const hasCupDimensional = availableColumns.some(col => col.includes('cup_handle_dimensional_scores'));
  const hasConfidenceLevel = availableColumns.includes('vcp_confidence_level') || availableColumns.includes('cup_handle_confidence_level');

  const formatDate = (dateValue: unknown): string => {
    if (!dateValue) return 'N/A';
    try {
      const date = new Date(dateValue as string);
      return date.toLocaleDateString('ko-KR');
    } catch {
      return String(dateValue);
    }
  };

  const formatNumber = (value: unknown): React.ReactNode => {
    if (value === null || value === undefined || value === 'N/A') {
      return <span className="text-gray-400 italic">N/A</span>;
    }
    const num = Number(value);
    if (isNaN(num)) return <span>{String(value)}</span>;
    return <span>{num.toFixed(2)}</span>;
  };

  const formatDimensionalScore = (score: unknown): string => {
    if (score == null || typeof score !== 'number' || isNaN(score)) {
      return 'N/A';
    }
    return `${(score * 100).toFixed(0)}%`;
  };

  const renderDimensionalScores = (scores: unknown, type: 'vcp' | 'cup_handle'): React.ReactNode => {
    if (!scores || typeof scores !== 'object' || scores === null) {
      return null;
    }
    
    const scoreObj = scores as Record<string, unknown>;
    
    return (
      <div className="grid grid-cols-2 gap-1 text-xs">
        <div className="text-gray-600">
          기술적: <span className="font-medium">{formatDimensionalScore(scoreObj.technical_quality)}</span>
        </div>
        <div className="text-gray-600">
          거래량: <span className="font-medium">{formatDimensionalScore(scoreObj.volume_confirmation)}</span>
        </div>
        <div className="text-gray-600">
          시간적: <span className="font-medium">{formatDimensionalScore(scoreObj.temporal_validity)}</span>
        </div>
        <div className="text-gray-600">
          시장: <span className="font-medium">{formatDimensionalScore(scoreObj.market_context)}</span>
        </div>
      </div>
    );
  };

  return (
    <div className="overflow-x-auto">
      <table className="min-w-full divide-y divide-gray-200">
        <thead className="bg-gray-50">
          <tr>
            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
              티커
            </th>
            {hasRsScore && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <div className="flex items-center gap-1">
                  <TrophyIcon className="h-4 w-4" />
                  RS 점수
                </div>
              </th>
            )}
            {hasSignalDate && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                <div className="flex items-center gap-1">
                  <CalendarIcon className="h-4 w-4" />
                  날짜
                </div>
              </th>
            )}
            {hasMetCount && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                충족 조건
              </th>
            )}
            {hasVcpDetected && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                VCP 패턴
              </th>
            )}
            {hasCupHandleDetected && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Cup & Handle
              </th>
            )}
            {hasVcpConfidence && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                VCP 신뢰도
              </th>
            )}
            {hasCupHandleConfidence && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                C&H 신뢰도
              </th>
            )}
            {hasConfidenceLevel && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                신뢰도 등급
              </th>
            )}
            {(hasVcpDimensional || hasCupDimensional) && (
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                다차원 평가
              </th>
            )}
            {/* 차트 컬럼 제거 - 행 클릭으로 통일 */}
          </tr>
        </thead>
        <tbody className="bg-white divide-y divide-gray-200">
          {data.map((item, index) => (
            <tr 
              key={`${item.symbol}-${index}`}
              className={`hover:bg-gray-50 transition-colors duration-200 ${
                onRowClick ? 'cursor-pointer' : ''
              }`}
              onClick={() => handleRowClick(item)}
            >
              <td className="px-6 py-4 whitespace-nowrap">
                <div className="flex items-center">
                  <div className="text-sm font-medium text-purple-600">
                    {String(item.symbol || 'N/A')}
                  </div>
                </div>
              </td>
              {hasRsScore && (
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex items-center">
                    {item.rs_score ? (
                      <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                        Number(item.rs_score) >= 90 
                          ? 'bg-green-100 text-green-800'
                          : Number(item.rs_score) >= 85
                          ? 'bg-blue-100 text-blue-800'
                          : 'bg-yellow-100 text-yellow-800'
                      }`}>
                        {formatNumber(item.rs_score)}
                      </span>
                    ) : (
                       <span className="text-gray-400 italic text-sm">N/A</span>
                     )}
                  </div>
                </td>
              )}
              {hasSignalDate && (
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                  {formatDate(item.signal_date || item.detection_date || item.processing_date)}
                </td>
              )}
              {hasMetCount && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {(item.met_count || item.fin_met_count) ? (
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      Number(item.met_count || item.fin_met_count) === 8
                        ? 'bg-green-100 text-green-800'
                        : Number(item.met_count || item.fin_met_count) >= 6
                        ? 'bg-blue-100 text-blue-800'
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {String(item.met_count || item.fin_met_count)}/8
                    </span>
                  ) : (
                    <span className="text-gray-400 italic text-sm">N/A</span>
                  )}
                </td>
              )}
              {hasVcpDetected && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {item.vcp_detected === true ? (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      ✓ 감지됨
                    </span>
                  ) : item.vcp_detected === false ? (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                      ✗ 미감지
                    </span>
                  ) : (
                    <span className="text-gray-400 italic text-sm">N/A</span>
                  )}
                </td>
              )}
              {hasCupHandleDetected && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {item.cup_handle_detected === true ? (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                      ✓ 감지됨
                    </span>
                  ) : item.cup_handle_detected === false ? (
                    <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                      ✗ 미감지
                    </span>
                  ) : (
                    <span className="text-gray-400 text-sm">N/A</span>
                  )}
                </td>
              )}
              {hasVcpConfidence && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {item.vcp_confidence !== null && item.vcp_confidence !== undefined ? (
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      Number(item.vcp_confidence) >= 0.7
                        ? 'bg-green-100 text-green-800'
                        : Number(item.vcp_confidence) >= 0.5
                        ? 'bg-yellow-100 text-yellow-800'
                        : 'bg-red-100 text-red-800'
                    }`}>
                      {(Number(item.vcp_confidence) * 100).toFixed(1)}%
                    </span>
                  ) : (
                    <span className="text-gray-400 text-sm">N/A</span>
                  )}
                </td>
              )}
              {hasCupHandleConfidence && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {item.cup_handle_confidence !== null && item.cup_handle_confidence !== undefined ? (
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      Number(item.cup_handle_confidence) >= 0.7
                        ? 'bg-green-100 text-green-800'
                        : Number(item.cup_handle_confidence) >= 0.5
                        ? 'bg-blue-100 text-blue-800'
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {(Number(item.cup_handle_confidence) * 100).toFixed(1)}%
                    </span>
                  ) : (
                    <span className="text-gray-400 text-sm">N/A</span>
                  )}
                </td>
              )}
              {hasConfidenceLevel && (
                <td className="px-6 py-4 whitespace-nowrap">
                  {(item.vcp_confidence_level || item.cup_handle_confidence_level) ? (
                    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                      (item.vcp_confidence_level === 'High' || item.cup_handle_confidence_level === 'High')
                        ? 'bg-green-100 text-green-800'
                        : (item.vcp_confidence_level === 'Medium' || item.cup_handle_confidence_level === 'Medium')
                        ? 'bg-blue-100 text-blue-800'
                        : (item.vcp_confidence_level === 'Low' || item.cup_handle_confidence_level === 'Low')
                        ? 'bg-yellow-100 text-yellow-800'
                        : 'bg-gray-100 text-gray-800'
                    }`}>
                      {String(item.vcp_confidence_level || item.cup_handle_confidence_level || 'N/A')}
                    </span>
                  ) : (
                    <span className="text-gray-400 text-sm">N/A</span>
                  )}
                </td>
              )}
              {(hasVcpDimensional || hasCupDimensional) && (
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="space-y-1">
                    {(item.vcp_dimensional_scores || item.cup_handle_dimensional_scores) ? (
                      <div className="text-xs space-y-1">
                        {renderDimensionalScores(item.vcp_dimensional_scores, 'vcp')}
                        {renderDimensionalScores(item.cup_handle_dimensional_scores, 'cup_handle')}
                      </div>
                    ) : (
                      <span className="text-gray-400 text-sm">N/A</span>
                    )}
                  </div>
                </td>
              )}
              {/* 차트 버튼 제거 - 행 클릭으로 통일 */}
            </tr>
          ))}
        </tbody>
      </table>
      
      {data.length > 0 && (
        <div className="px-6 py-3 bg-gray-50 border-t border-gray-200">
          <p className="text-sm text-gray-700">
            총 <span className="font-medium">{data.length}</span>개 종목 표시
            {onRowClick && (
              <span className="text-gray-500 ml-2">
                • 행을 클릭하여 TradingView 차트를 확인하세요
              </span>
            )}
          </p>
        </div>
      )}
    </div>
  );
};

export default EnhancedDataTable;